const byId = (id) => document.getElementById(id);
const out = {
  lender: byId('lenderOut'),
  agent: byId('agentOut'),
  manager: byId('managerOut'),
  hr: byId('hrOut'),
};

function setStatus(msg) {
  const el = document.getElementById('status');
  if (el) el.textContent = msg || '';
}

function apiBase() {
  return document.getElementById('apiBase').value.trim().replace(/\/$/, '');
}

aSyncHandlers();
attachActions();

function aSyncHandlers() {
  document.getElementById('saveBase').addEventListener('click', () => {
    setStatus('Base set.');
  });
}

function attachActions() {
  document.querySelectorAll('button[data-action]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const action = btn.getAttribute('data-action');
      run(action).catch((err) => {
        console.error(err);
        setPanel(action, { error: err.message || String(err) });
      });
    });
  });
}

async function run(action) {
  if (action === 'lender') return fetchLender();
  if (action === 'agent') return fetchAgent();
  if (action === 'manager') return fetchManager();
  if (action === 'hr') return fetchHr();
}

function setPanel(key, data) {
  const target = out[key];
  if (!target) return;
  target.textContent = JSON.stringify(data, null, 2);
}

async function fetchLender() {
  const lender_id = byId('lenderId').value.trim();
  const bucket_filter = byId('lenderBucket').value.trim();
  if (!lender_id) return setPanel('lender', { error: 'lender_id is required' });
  const url = new URL(apiBase() + '/dashboard/lender/portfolio-summary');
  url.searchParams.set('lender_id', lender_id);
  if (bucket_filter) url.searchParams.set('bucket_filter', bucket_filter);
  setPanel('lender', { loading: true });
  const res = await fetch(url);
  const data = await res.json();
  setPanel('lender', data);
}

async function fetchAgent() {
  const agent_id = byId('agentId').value.trim();
  const status_filter = byId('agentStatus').value.trim();
  if (!agent_id) return setPanel('agent', { error: 'agent_id is required' });
  const url = new URL(apiBase() + '/dashboard/agent/assigned-loans');
  url.searchParams.set('agent_id', agent_id);
  if (status_filter) url.searchParams.set('status_filter', status_filter);
  setPanel('agent', { loading: true });
  const res = await fetch(url);
  const data = await res.json();
  setPanel('agent', data);
}

async function fetchManager() {
  const branch_id = byId('branchId').value.trim();
  const date = byId('branchDate').value.trim();
  if (!branch_id) return setPanel('manager', { error: 'branch_id is required' });
  const url = new URL(apiBase() + '/dashboard/manager/branch-summary');
  url.searchParams.set('branch_id', branch_id);
  if (date) url.searchParams.set('date', date);
  setPanel('manager', { loading: true });
  const res = await fetch(url);
  const data = await res.json();
  setPanel('manager', data);
}

async function fetchHr() {
  const url = new URL(apiBase() + '/dashboard/hr/performance');
  setPanel('hr', { loading: true });
  const res = await fetch(url);
  const data = await res.json();
  setPanel('hr', data);
}
