const state = {
  templates: [],
  selectedTemplate: null,
  activeJobId: null,
  eventSource: null,
};

const templateSelect = document.getElementById('templateSelect');
const paramsForm = document.getElementById('paramsForm');
const submitBtn = document.getElementById('submitBtn');
const submittedByInput = document.getElementById('submittedBy');
const submitResult = document.getElementById('submitResult');
const jobIdNode = document.getElementById('jobId');
const currentStageNode = document.getElementById('currentStage');
const timelineNode = document.getElementById('timeline');
const eventsNode = document.getElementById('events');

async function loadTemplates() {
  const response = await fetch('/api/templates');
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

  const response = await fetch('/api/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ template_id: templateId, submitted_by: submittedBy, params }),
  });

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
  const response = await fetch(`/api/jobs/${jobId}`);
  if (!response.ok) {
    return;
  }
  const payload = await response.json();
  currentStageNode.textContent = payload.stage_model?.current || '-';
  renderTimeline(payload.stage_model?.timeline || []);
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

loadTemplates().catch((error) => {
  submitResult.textContent = JSON.stringify({ error: error.message }, null, 2);
});
