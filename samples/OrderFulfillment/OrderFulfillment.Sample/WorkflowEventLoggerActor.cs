using Akka.Actor;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;

namespace OrderFulfillment.Sample;

/// <summary>
/// Turns every <see cref="WorkflowFeedItem"/> into one structured log line — the live "what is this
/// workflow doing right now" feed, which flows through the OTLP log exporter into the Aspire
/// dashboard's Structured Logs view — and writes the same event into
/// <see cref="OrderReadModelRepository"/>'s Postgres tables, the data the Razor Pages UI renders
/// from. A step's completion/failure and a workflow's end also re-query the workflow's own
/// authoritative state (<see cref="GetOrderState"/>/<see cref="GetItemState"/> depending on
/// <see cref="WorkflowFeedItem.WorkflowType"/>) so the read model's status is sourced from the
/// workflow itself.
///
/// One instance of this actor runs per replica, each subscribed to the same cluster-wide
/// <see cref="Sagant.Runtime.Akka.Clustering.WorkflowEventPubSubBridge"/> topic (see this
/// project's <c>Program.cs</c>) — every replica sees every order's full event stream regardless of
/// which replica placed it or currently hosts its entity, and every replica's write lands in the
/// same shared Postgres tables. <see cref="OrderPlacementService"/> has already written this
/// workflow's (and, for an order, every one of its items') <c>workflow_views</c> row before any
/// command is even sent — see <see cref="OrderReadModelRepository.PlaceOrderAsync"/> — so there is
/// no "create on first sight" case to handle here.
///
/// One <c>ReceiveAsync</c> over one type, because every published item carries a
/// <see cref="WorkflowEvent"/> and every event names what drove it through
/// <see cref="TransitionCause"/>. A step's name, attempt, duration and error all arrive on the cause
/// of the event that step produced.
/// </summary>
internal sealed class WorkflowEventLoggerActor : ReceiveActor
{
    public WorkflowEventLoggerActor(
        ILogger logger, OrderReadModelRepository repo, OrderChangeSignal changeSignal)
    {
        // Orders this replica has already registered, so the check above costs nothing after the
        // first event about one.
        var registered = new HashSet<string>();

        async Task Record(WorkflowFeedItem item, string what, Func<Task> write)
        {
            try
            {
                await write();
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "failed to record {What} for {EntityId}", what, item.EntityId);
            }
        }

        ReceiveAsync<WorkflowFeedItem>(async item =>
        {
            // This read model is about orders, and every row it writes hangs off a workflow_views row
            // OrderPlacementService created before the run began. A workflow that never went through
            // that path — a schedule, say — has no such row, so its events belong to whatever reads
            // its own history rather than to this one.
            if (item.WorkflowType is not (nameof(OrderFulfillmentWorkflow) or nameof(ItemFulfillmentWorkflow)))
            {
                return;
            }

            var id = item.EntityId;
            var at = item.Timestamp;

            // An order this replica did not place — one a schedule started — has none of the rows the
            // writes below hang off, so it is registered on first sight. Once per id per replica,
            // since a schedule places one every couple of minutes and this is a read model, not a
            // hot path.
            if (registered.Add(id))
            {
                if (item.WorkflowType == nameof(OrderFulfillmentWorkflow))
                {
                    await Record(item, "registration", () => repo.EnsureOrderRegistered(id, "scheduled"));
                }
                else if (item.WorkflowType == nameof(ItemFulfillmentWorkflow)
                         && id.IndexOf('#', StringComparison.Ordinal) is > 0 and var separator)
                {
                    // An item is scoped to its order, so the order it belongs to is the part of its id
                    // before the separator — which is what makes an item a schedule placed reachable
                    // from the order the UI renders.
                    await Record(item, "registration", () => repo.EnsureItemRegistered(id, id[..separator]));
                }
            }

            // The cause is what the workflow was reacting to; the event is what it did about it.
            // Reporting them separately keeps a retried step's error visible even though the run
            // goes on to succeed.
            if (item.Event is WorkflowEvent.CausedEvent caused)
            {
                switch (caused.Cause)
                {
                    case TransitionCause.Command command:
                        logger.LogInformation("{EntityId}: command {CommandType} handled", id, command.CommandType);

                        await Record(item, "log line", () =>
                            repo.RecordLogLine(id, at, $"{at:T} command {command.CommandType} handled"));
                        changeSignal.Raise();
                        break;

                    case TransitionCause.StepSucceeded s:
                        logger.LogInformation(
                            "{EntityId}: step {StepName} completed in {Duration} (attempt {Attempt})",
                            id, s.StepName, s.Duration, s.Attempt);
                        await Record(item, "step-completed", async () =>
                        {
                            await repo.RecordStepCompleted(id, s.StepName, s.Attempt);
                            await repo.RecordLogLine(id, at, $"{at:T} {s.StepName} completed in {s.Duration}");
                        });
                        changeSignal.Raise();
                        break;

                    case TransitionCause.StepFailed f:
                        logger.LogWarning(
                            "{EntityId}: step {StepName} failed on attempt {Attempt} ({Error}), retrying = {WillRetry}",
                            id, f.StepName, f.Attempt, f.Error, f.WillRetry);
                        await Record(item, "step-failed", async () =>
                        {
                            await repo.RecordStepFailed(id, f.StepName, f.Attempt, f.Error);
                            await repo.RecordLogLine(id, at,
                                $"{at:T} {f.StepName} failed (attempt {f.Attempt}): {f.Error}" +
                                (f.WillRetry ? " — retrying" : " — failing over"));
                        });

                        changeSignal.Raise();
                        break;
                }
            }

            switch (item.Event)
            {
                // The workflow's own state travels in the feed, so its status reaches the read model
                // at the moment it changes and costs no round trip to the entity.
                case WorkflowEvent.UserStateChanged<OrderState> order:
                    await Record(item, "status", () =>
                        repo.RefreshStatus(id, order.State.Status.ToString(), order.State.FailureReason));
                    changeSignal.Raise();
                    break;

                case WorkflowEvent.UserStateChanged<ItemState> itemState:
                    await Record(item, "status", () =>
                        repo.RefreshStatus(id, itemState.State.Status.ToString(), null));
                    changeSignal.Raise();
                    break;

                case WorkflowEvent.StepStarted started:
                    // Always attempt 1: entering a step resets its retry count, and a retry re-runs
                    // the same step under StepRetryScheduled below without starting it afresh.
                    logger.LogInformation("{EntityId}: step {StepName} started", id, started.StepName);
                    await Record(item, "step-started", async () =>
                    {
                        await repo.RecordStepStarted(id, started.StepName, 1, at);
                        await repo.RecordLogLine(id, at, $"{at:T} {started.StepName} started");
                    });
                    changeSignal.Raise();
                    break;

                case WorkflowEvent.StepRetryScheduled retry:
                    logger.LogInformation("{EntityId}: retry {RetryCount} scheduled", id, retry.RetryCount);
                    await Record(item, "step-started", () =>
                        repo.RecordStepStarted(
                            id, ((TransitionCause.StepFailed)retry.Cause).StepName, retry.RetryCount + 1, at));
                    changeSignal.Raise();
                    break;

                case WorkflowEvent.RunPaused paused:
                    logger.LogInformation("{EntityId}: paused ({Reason})", id, paused.Reason);
                    await Record(item, "pause", async () =>
                    {
                        await repo.SetPaused(id, paused.Reason, at + OrderFulfillmentWorkflow.ApprovalPauseTimeout);
                        await repo.RecordLogLine(id, at, $"{at:T} paused ({paused.Reason})");
                    });
                    changeSignal.Raise();
                    break;

                case WorkflowEvent.RunResumed:
                    logger.LogInformation("{EntityId}: resumed", id);
                    await Record(item, "resume", async () =>
                    {
                        await repo.SetResumed(id);
                        await repo.RecordLogLine(id, at, $"{at:T} resumed");
                    });
                    changeSignal.Raise();
                    break;

                case WorkflowEvent.RunFinished finished:
                    logger.LogInformation("{EntityId}: finished — {Outcome}", id, Describe(finished.Outcome));
                    try
                    {
                        await repo.RecordLogLine(id, at, $"{at:T} finished — {Describe(finished.Outcome)}");

                        // A run stopped from outside reaches no business status of its own, so the
                        // outcome supplies one. Every other ending arrives through the workflow's
                        // last UserStateChanged above.
                        if (finished.Outcome is WorkflowOutcome.Terminated)
                        {
                            await repo.RefreshStatus(id, "Terminated", Describe(finished.Outcome));
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "{EntityId}: failed to record its finish", id);
                    }

                    changeSignal.Raise();
                    break;

                case WorkflowEvent.RunDeleted deleted:
                    logger.LogInformation("{EntityId}: deleted", id);
                    await repo.SoftDeleteAsync(id);
                    changeSignal.Raise();
                    break;
            }
        });
    }

    /// <summary>Renders an outcome for a log line and the read model's status column. Each case says
    /// something different, which is the whole point of them being distinct.</summary>
    private static string Describe(WorkflowOutcome outcome) => outcome switch
    {
        WorkflowOutcome.Completed => "completed",
        WorkflowOutcome.Failed f => $"failed: {f.Cause}",
        WorkflowOutcome.TimedOut => "timed out",
        WorkflowOutcome.Terminated t => t.Reason is { } r ? $"terminated: {r}" : "terminated",
        _ => "finished",
    };
}
