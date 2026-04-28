const state = {
  templates: [],
  selectedTemplate: null,
  activeJobId: null,
  eventSource: null,
  accessToken: null,
  refreshToken: null,
  loginStatus: 'logged_out',
  loginError: null,
};

const authStatusNode = document.getElementById('authStatus');
const loginUsernameInput = document.getElementById('loginUsername');
const loginPasswordInput = document.getElementById('loginPassword');
const loginBtn = document.getElementById('loginBtn');
const loginResult = document.getElementById('loginResult');
const templateSelect = document.getElementById('templateSelect');
const paramsForm = document.getElementById('paramsForm');
const submitBtn = document.getElementById('submitBtn');
const submittedByInput = document.getElementById('submittedBy');
const submitResult = document.getElementById('submitResult');
const jobIdNode = document.getElementById('jobId');
const currentStageNode = document.getElementById('currentStage');
const timelineNode = document.getElementById('timeline');
const eventsNode = document.getElementById('events');
const opsHealthNode = document.getElementById('opsHealth');
const opsStageMetricsNode = document.getElementById('opsStageMetrics');
const spoolSectionTypeNode = document.getElementById('spoolSectionType');
const spoolQueryNode = document.getElementById('spoolQuery');
const refreshSpoolBtn = document.getElementById('refreshSpoolBtn');
const downloadSpoolBtn = document.getElementById('downloadSpoolBtn');
const spoolMetaNode = document.getElementById('spoolMeta');
const spoolContentNode = document.getElementById('spoolContent');

function setLoginStatus(status, message = '') {
  state.loginStatus = status;
  state.loginError = message;
  const labels = {
    logged_out: 'Logged out',
    logging_in: 'Logging in...',
    login_failed: 'Login failed',
    logged_in: 'Logged in',
    token_expired: 'Token expired',
  };
  authStatusNode.textContent = labels[status] || status;
  loginResult.textContent = message;
}

function clearAuthState(expired = false) {
  state.accessToken = null;
  state.refreshToken = null;
  setLoginStatus(expired ? 'token_expired' : 'logged_out', expired ? 'Session expired. Please log in again.' : '');
}

async function login() {
  const username = loginUsernameInput.value.trim();
  const password = loginPasswordInput.value;
  if (!username || !password) {
    setLoginStatus('login_failed', 'Username and password are required.');
    return;
  }

  setLoginStatus('logging_in');
  const response = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    clearAuthState(false);
    setLoginStatus('login_failed', payload?.detail?.message || 'Invalid credentials.');
    return;
  }

  state.accessToken = payload.access_token || null;
  state.refreshToken = payload.refresh_token || null;
  loginPasswordInput.value = '';
  setLoginStatus('logged_in', `Authenticated as ${payload.username || username}.`);
}

async function refreshAccessToken() {
  if (!state.refreshToken) {
    clearAuthState(true);
    return false;
  }
  const response = await fetch('/api/refresh', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refresh_token: state.refreshToken }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    clearAuthState(true);
    return false;
  }
  state.accessToken = payload.access_token || null;
  if (payload.refresh_token) {
    state.refreshToken = payload.refresh_token;
  }
  setLoginStatus('logged_in', `Session refreshed for ${payload.username || loginUsernameInput.value.trim() || 'user'}.`);
  return true;
}

async function apiFetch(url, options = {}, requiresAuth = false) {
  const headers = new Headers(options.headers || {});
  if (requiresAuth) {
    if (!state.accessToken) {
      clearAuthState(false);
      throw new Error('Not authenticated. Please log in.');
    }
    headers.set('Authorization', `Bearer ${state.accessToken}`);
  }

  const requestOptions = { ...options, headers };
  let response = await fetch(url, requestOptions);
  if (response.status === 401 && requiresAuth) {
    const refreshed = await refreshAccessToken();
    if (!refreshed || !state.accessToken) {
      throw new Error('Authentication expired. Please log in again.');
    }
    headers.set('Authorization', `Bearer ${state.accessToken}`);
    response = await fetch(url, { ...options, headers });
    if (response.status === 401) {
      clearAuthState(true);
      throw new Error('Authentication failed after token refresh. Please log in again.');
    }
  }
  return response;
}

async function loadTemplates() {
  const response = await apiFetch('/api/templates');
  if (!response.ok) {
    throw new Error(`Failed to load templates: ${response.status}`);
  }
  const payload = await response.json();
  state.templates = payload.templates || [];
  renderTemplateOptions();
}

function renderTemplateOptions() {
  templateSelect.innerHTML = '';
  for (const template of state.templates) {
    const option = document.createElement('option');
    option.value = template.template_id;
    option.textContent = `${template.template_id} — ${template.description || ''}`;
    templateSelect.appendChild(option);
  }

  const defaultTemplateId = state.templates[0]?.template_id;
  if (defaultTemplateId) {
    templateSelect.value = defaultTemplateId;
    setSelectedTemplate(defaultTemplateId);
  }
}

function setSelectedTemplate(templateId) {
  state.selectedTemplate = state.templates.find((t) => t.template_id === templateId) || null;
  renderParamInputs();
}

function renderParamInputs() {
  paramsForm.innerHTML = '';
  if (!state.selectedTemplate) {
    return;
  }

  for (const [paramName, schema] of Object.entries(state.selectedTemplate.params || {})) {
    const wrapper = document.createElement('div');

    const label = document.createElement('label');
    label.setAttribute('for', `param_${paramName}`);
    label.textContent = `${paramName}${schema.required ? ' *' : ''}`;

    const input = document.createElement('input');
    input.id = `param_${paramName}`;
    input.name = paramName;
    input.required = Boolean(schema.required);
    input.placeholder = schema.help || '';
    if (schema.default !== undefined && schema.default !== null) {
      input.value = String(schema.default);
    }

    wrapper.appendChild(label);
    wrapper.appendChild(input);
    paramsForm.appendChild(wrapper);
  }
}

function collectParams() {
  const payload = {};
  const fields = paramsForm.querySelectorAll('input');
  for (const field of fields) {
    const value = field.value.trim();
    if (value) {
      payload[field.name] = value;
    }
  }
  return payload;
}

async function submitJob() {
  const templateId = templateSelect.value;
  const submittedBy = submittedByInput.value.trim() || 'web-ui';
  const params = collectParams();

  const response = await apiFetch('/api/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ template_id: templateId, submitted_by: submittedBy, params }),
  }, true);

  const payload = await response.json();
  submitResult.textContent = JSON.stringify(payload, null, 2);

  if (!response.ok) {
    return;
  }

  state.activeJobId = payload.id;
  jobIdNode.textContent = payload.id;
  renderTimeline(payload.stage_model?.timeline || []);
  currentStageNode.textContent = payload.stage_model?.current || '-';
  startEventStream(payload.id);
  refreshJobStatus(payload.id);
  refreshSpoolView(payload.id);
}

function renderTimeline(items) {
  timelineNode.innerHTML = '';
  for (const item of items) {
    const line = document.createElement('li');
    line.textContent = `${item.stage} @ ${item.first_seen_at}`;
    timelineNode.appendChild(line);
  }
}

function addEventLine(eventType, data) {
  const line = document.createElement('li');
  line.textContent = `${eventType}: ${JSON.stringify(data)}`;
  eventsNode.prepend(line);
  while (eventsNode.children.length > 40) {
    eventsNode.removeChild(eventsNode.lastChild);
  }
}

async function refreshJobStatus(jobId) {
  const response = await apiFetch(`/api/jobs/${jobId}`);
  if (!response.ok) {
    return;
  }
  const payload = await response.json();
  currentStageNode.textContent = payload.stage_model?.current || '-';
  renderTimeline(payload.stage_model?.timeline || []);
  refreshOpsDashboard();
}

async function refreshOpsDashboard() {
  const response = await apiFetch('/api/ops/dashboard');
  if (!response.ok) {
    opsHealthNode.textContent = 'Unable to load ops dashboard.';
    return;
  }
  const payload = await response.json();
  const health = payload.health || {};
  opsHealthNode.textContent = `Status=${health.status || 'unknown'} · jobs=${health.total_jobs || 0} · failed=${health.failed_jobs || 0} · failure_rate=${health.failure_rate || 0}`;
  opsStageMetricsNode.innerHTML = '';
  for (const item of payload.stage_metrics || []) {
    const line = document.createElement('li');
    line.textContent = `${item.stage}: avg=${item.avg_ms}ms p95=${item.p95_ms}ms max=${item.max_ms}ms (n=${item.samples})`;
    opsStageMetricsNode.appendChild(line);
  }
}

function spoolUrl(jobId, asText = false) {
  const endpoint = asText ? 'text' : '';
  const base = endpoint ? `/api/jobs/${jobId}/spool/${endpoint}` : `/api/jobs/${jobId}/spool`;
  const search = new URLSearchParams();
  const sectionType = spoolSectionTypeNode.value;
  const query = spoolQueryNode.value.trim();
  if (sectionType) {
    search.set('section_type', sectionType);
  }
  if (query) {
    search.set('query', query);
  }
  const suffix = search.toString();
  return suffix ? `${base}?${suffix}` : base;
}

async function refreshSpoolView(jobId = state.activeJobId) {
  if (!jobId) {
    spoolMetaNode.textContent = 'No active job selected.';
    spoolContentNode.textContent = '';
    return;
  }
  const response = await apiFetch(spoolUrl(jobId));
  if (!response.ok) {
    spoolMetaNode.textContent = `Spool unavailable for job ${jobId}.`;
    spoolContentNode.textContent = '';
    return;
  }
  const payload = await response.json();
  const sections = payload.sections || [];
  spoolMetaNode.textContent = `Showing ${sections.length} section(s) for job ${jobId}.`;
  spoolContentNode.textContent = sections
    .map((item) => `===== ${item.section_type.toUpperCase()} #${item.ordinal} =====\n${item.content_text}`)
    .join('\n\n');
}

function downloadSpool(jobId = state.activeJobId) {
  if (!jobId) {
    spoolMetaNode.textContent = 'No active job selected.';
    return;
  }
  window.open(spoolUrl(jobId, true), '_blank');
}

function startEventStream(jobId) {
  if (state.eventSource) {
    state.eventSource.close();
  }
  eventsNode.innerHTML = '';

  state.eventSource = new EventSource(`/api/jobs/${jobId}/events`);
  state.eventSource.onmessage = (event) => {
    try {
      addEventLine('message', JSON.parse(event.data));
    } catch {
      addEventLine('message', { raw: event.data });
    }
    refreshJobStatus(jobId);
  };

  state.eventSource.addEventListener('job.created', (event) => {
    addEventLine('job.created', JSON.parse(event.data));
  });

  state.eventSource.onerror = () => {
    addEventLine('stream.error', { message: 'Event stream disconnected or unavailable' });
  };
}

templateSelect.addEventListener('change', (event) => setSelectedTemplate(event.target.value));
submitBtn.addEventListener('click', () => {
  submitJob().catch((error) => {
    submitResult.textContent = JSON.stringify({ error: error.message }, null, 2);
  });
});
loginBtn.addEventListener('click', () => {
  login().catch((error) => {
    clearAuthState(false);
    setLoginStatus('login_failed', error.message);
  });
});
refreshSpoolBtn.addEventListener('click', () => {
  refreshSpoolView().catch((error) => {
    spoolMetaNode.textContent = `Error loading spool: ${error.message}`;
  });
});
downloadSpoolBtn.addEventListener('click', () => downloadSpool());

loadTemplates().catch((error) => {
  submitResult.textContent = JSON.stringify({ error: error.message }, null, 2);
});

refreshOpsDashboard();
setInterval(() => {
  refreshOpsDashboard().catch(() => {});
}, 10000);
setLoginStatus('logged_out');
