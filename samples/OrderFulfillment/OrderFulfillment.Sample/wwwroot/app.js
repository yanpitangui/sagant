(() => {
    let selectedOrderId = new URLSearchParams(location.search).get('order');
    let countdownInterval = null;

    async function refreshList() {
        const res = await fetch(`/fragments/order-list?selected=${encodeURIComponent(selectedOrderId ?? '')}`);
        document.getElementById('order-list').outerHTML = await res.text();
    }

    async function refreshDetail() {
        if (!selectedOrderId) {
            return;
        }
        const res = await fetch(`/fragments/order-detail/${encodeURIComponent(selectedOrderId)}`);
        document.getElementById('order-detail').outerHTML = await res.text();
        startCountdowns();
    }

    function selectOrder(orderId) {
        selectedOrderId = orderId;
        const url = new URL(location.href);
        url.searchParams.set('order', orderId);
        history.replaceState(null, '', url);
        refreshList();
        refreshDetail();
    }

    function startCountdowns() {
        if (countdownInterval) {
            clearInterval(countdownInterval);
            countdownInterval = null;
        }
        function tick() {
            document.querySelectorAll('.countdown[data-deadline]').forEach((el) => {
                const remainingSeconds = Math.max(0, Math.floor((Number(el.dataset.deadline) - Date.now()) / 1000));
                const mm = String(Math.floor(remainingSeconds / 60)).padStart(2, '0');
                const ss = String(remainingSeconds % 60).padStart(2, '0');
                el.textContent = `auto-cancels in ${mm}:${ss}`;
            });
        }
        if (document.querySelector('.countdown[data-deadline]')) {
            tick();
            countdownInterval = setInterval(tick, 1000);
        }
    }

    document.addEventListener('click', (e) => {
        const row = e.target.closest('[data-order-id]');
        if (row) {
            selectOrder(row.dataset.orderId);
        }
    });

    // A failed POST (antiforgery rejection, an unhandled exception in the handler, a workflow
    // command timing out) otherwise fails completely silently — no visible change, nothing in the
    // browser console — which reads exactly like "the button did nothing". Surfacing the status
    // code and body at least gets it into the console instead of disappearing.
    async function postForm(form) {
        const res = await fetch(form.action, { method: 'POST', body: new FormData(form) });
        if (!res.ok) {
            const body = await res.text().catch(() => '');
            console.error(`POST ${form.action} failed: ${res.status} ${res.statusText}`, body);
            alert(`Action failed (${res.status}). Check the browser console for details.`);
        }
        return res;
    }

    document.addEventListener('submit', async (e) => {
        const approveForm = e.target.closest('[data-approve-form]');
        if (approveForm) {
            e.preventDefault();
            await postForm(approveForm);
            refreshDetail();
            return;
        }

        const deleteForm = e.target.closest('[data-delete-form]');
        if (deleteForm) {
            e.preventDefault();
            await postForm(deleteForm);
            refreshList();
            refreshDetail();
            return;
        }

        const placeForm = e.target.closest('#place-order-form');
        if (placeForm) {
            e.preventDefault();
            const res = await postForm(placeForm);
            if (res.ok) {
                const { orderId } = await res.json();
                selectOrder(orderId);
            }
        }
    });

    // "+ add item" clones the first item row (a plain input[name=itemAmounts], repeatable — the
    // form posts one array-bound value per row); "×" removes its own row, but never the last one
    // left — an order always needs at least one line item.
    document.addEventListener('click', (e) => {
        if (e.target.closest('#add-item-button')) {
            const rows = document.getElementById('item-rows');
            const clone = rows.firstElementChild.cloneNode(true);
            clone.querySelector('.amount-input').value = 500;
            rows.appendChild(clone);
            return;
        }

        const removeButton = e.target.closest('.remove-item-button');
        if (removeButton) {
            const rows = document.getElementById('item-rows');
            if (rows.children.length > 1) {
                removeButton.closest('.item-row').remove();
            }
        }
    });

    const events = new EventSource('/orders/stream');
    events.onmessage = () => {
        refreshList();
        refreshDetail();
    };

    startCountdowns();
})();
