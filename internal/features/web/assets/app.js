let events = [];
let selectedEvent = null;
let currentTab = 'payload';

document.addEventListener('DOMContentLoaded', () => {
  initStatus();
  initEvents();
  initSSE();
  initTabs();
  initSearch();
});

async function initStatus() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    document.getElementById('tunnel-url').textContent = data.public_url || 'Connecting...';
    document.getElementById('local-target').textContent = data.local_target || 'http://localhost:8080';
    document.getElementById('tunnel-id').textContent = data.tunnel_id || '';
  } catch (err) {
    console.error('Failed to load status:', err);
  }
}

async function initEvents() {
  try {
    const res = await fetch('/api/events');
    const data = await res.json();
    events = data || [];
    renderEventList();
    if (events.length > 0 && !selectedEvent) {
      selectEvent(events[0]);
    }
  } catch (err) {
    console.error('Failed to load events:', err);
  }
}

function initSSE() {
  const evtSource = new EventSource('/api/events/stream');
  evtSource.onmessage = (e) => {
    try {
      const evt = JSON.parse(e.data);
      events.unshift(evt);
      if (events.length > 500) events.pop();
      renderEventList();
      if (!selectedEvent) {
        selectEvent(evt);
      }
    } catch (err) {
      console.error('Error parsing SSE event:', err);
    }
  };
  evtSource.onerror = () => {
    console.warn('SSE connection lost, reconnecting...');
  };
}

function renderEventList() {
  const listEl = document.getElementById('event-list');
  const searchVal = document.getElementById('search-input').value.toLowerCase();
  listEl.innerHTML = '';

  const filtered = events.filter(e => {
    return e.request.path.toLowerCase().includes(searchVal) ||
           e.provider.toLowerCase().includes(searchVal) ||
           e.request.method.toLowerCase().includes(searchVal);
  });

  if (filtered.length === 0) {
    listEl.innerHTML = '<li style="padding: 20px; text-align: center; color: var(--text-muted); font-size: 0.85rem;">No webhooks received yet.</li>';
    return;
  }

  filtered.forEach(evt => {
    const li = document.createElement('li');
    li.className = 'event-item' + (selectedEvent && selectedEvent.id === evt.id ? ' active' : '');
    li.onclick = () => selectEvent(evt);

    const statusClass = evt.response.status_code >= 200 && evt.response.status_code < 300 ? 'pill-status-2xx' :
                        evt.response.status_code >= 400 && evt.response.status_code < 500 ? 'pill-status-4xx' : 'pill-status-5xx';

    const timeStr = new Date(evt.timestamp).toLocaleTimeString();

    li.innerHTML = `
      <div class="event-header">
        <div style="display: flex; gap: 6px; align-items: center;">
          <span class="pill pill-method">${evt.request.method}</span>
          <span class="pill ${statusClass}">${evt.response.status_code || '---'}</span>
          <span class="pill pill-provider">${evt.provider.toUpperCase()}</span>
        </div>
        <span style="font-size: 0.75rem; color: var(--text-muted); font-family: var(--font-mono);">${timeStr}</span>
      </div>
      <div class="event-path">${escapeHtml(evt.request.path)}</div>
    `;
    listEl.appendChild(li);
  });
}

function selectEvent(evt) {
  selectedEvent = evt;
  renderEventList();
  renderDetail();
}

function renderDetail() {
  const container = document.getElementById('detail-container');
  if (!selectedEvent) {
    container.innerHTML = '<div style="padding: 40px; text-align: center; color: var(--text-muted);">Select a webhook from the left to inspect.</div>';
    return;
  }

  // Header meta bar
  document.getElementById('detail-method').textContent = selectedEvent.request.method;
  document.getElementById('detail-path').textContent = selectedEvent.request.path;
  document.getElementById('detail-meta').textContent = `${selectedEvent.provider.toUpperCase()} • ${selectedEvent.duration_ms}ms • ${new Date(selectedEvent.timestamp).toLocaleString()}`;

  renderActiveTab();
}

function initTabs() {
  document.querySelectorAll('.tab').forEach(tab => {
    tab.onclick = () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      currentTab = tab.dataset.tab;
      renderActiveTab();
    };
  });
}

function renderActiveTab() {
  const content = document.getElementById('tab-content');
  if (!selectedEvent) return;

  if (currentTab === 'payload') {
    let jsonFormatted = '';
    try {
      const parsed = JSON.parse(new TextDecoder().decode(base64ToUint8(selectedEvent.request.body)));
      jsonFormatted = JSON.stringify(parsed, null, 2);
    } catch {
      jsonFormatted = atob(selectedEvent.request.body || '');
    }
    content.innerHTML = `<pre class="code-block">${escapeHtml(jsonFormatted)}</pre>`;
  } else if (currentTab === 'headers') {
    let rows = '';
    const headers = selectedEvent.request.headers || {};
    let idx = 0;
    for (const [k, v] of Object.entries(headers)) {
      const fullVal = v.join(', ');
      const isSens = isSensitiveHeader(k);
      const maskedVal = isSens ? maskSecretValue(fullVal) : fullVal;
      idx++;
      const valHtml = isSens ? `
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <span id="hdr-val-${idx}" data-masked="true" data-original="${escapeHtml(fullVal)}" data-masked-val="${escapeHtml(maskedVal)}" style="font-family: var(--font-mono); font-size: 0.85rem;">${escapeHtml(maskedVal)}</span>
          <button class="btn btn-secondary" style="padding: 2px 6px; font-size: 0.75rem; margin-left: 8px;" onclick="toggleHeaderVisibility(${idx})">👁️ Reveal</button>
        </div>
      ` : `<span style="font-family: var(--font-mono); font-size: 0.85rem;">${escapeHtml(fullVal)}</span>`;

      rows += `<tr><td style="font-family: var(--font-mono); font-weight: 600; width: 220px;">${escapeHtml(k)}</td><td>${valHtml}</td></tr>`;
    }
    content.innerHTML = `<table class="table"><thead><tr><th>Header</th><th>Value</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (currentTab === 'response') {
    let respBody = '';
    try {
      respBody = atob(selectedEvent.response.body || '');
    } catch {}
    content.innerHTML = `
      <div style="margin-bottom: 12px; font-family: var(--font-mono); font-size: 0.9rem;">
        Status: <strong>${selectedEvent.response.status_code}</strong>
      </div>
      <pre class="code-block">${escapeHtml(respBody || (selectedEvent.response.error || 'No response body'))}</pre>
    `;
  } else if (currentTab === 'mutate') {
    let rawBody = '';
    try {
      const parsed = JSON.parse(new TextDecoder().decode(base64ToUint8(selectedEvent.request.body)));
      rawBody = JSON.stringify(parsed, null, 2);
    } catch {
      rawBody = atob(selectedEvent.request.body || '');
    }

    content.innerHTML = `
      <div class="mutation-form">
        <div class="form-group">
          <label>Payload (JSON)</label>
          <textarea id="mutate-body" class="form-control">${escapeHtml(rawBody)}</textarea>
        </div>
        <div class="form-group">
          <label>Signing Secret (Optional HMAC recalculation for Stripe, GitHub, etc.)</label>
          <input id="mutate-secret" class="form-control" type="text" placeholder="whsec_... or secret token" />
        </div>
        <div style="display: flex; gap: 10px;">
          <button class="btn" onclick="submitReplay(true)">Mutate & Replay</button>
          <button class="btn btn-secondary" onclick="submitReplay(false)">Replay Original</button>
        </div>
        <div id="replay-feedback" style="font-family: var(--font-mono); font-size: 0.85rem; margin-top: 10px;"></div>
        <div id="replay-diff-box" style="margin-top: 14px;"></div>
      </div>
    `;
  } else if (currentTab === 'composer') {
    renderComposerTab(content);
  } else if (currentTab === 'catalog') {
    renderCatalogTab(content);
  }
}

function renderComposerTab(container) {
  container.innerHTML = `
    <div style="max-width: 800px;">
      <h3 style="font-size: 1.1rem; margin-bottom: 4px;">Custom Webhook Composer (Scratchpad)</h3>
      <p style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 16px;">
        Craft and dispatch custom webhook payloads directly to your local endpoint with instant cryptographic HMAC signing.
      </p>
      <div style="display: grid; grid-template-columns: 120px 1fr; gap: 10px; margin-bottom: 12px;">
        <div class="form-group" style="margin: 0;">
          <label>Method</label>
          <select id="composer-method" class="form-control" style="height: 38px;">
            <option value="POST" selected>POST</option>
            <option value="GET">GET</option>
            <option value="PUT">PUT</option>
            <option value="PATCH">PATCH</option>
            <option value="DELETE">DELETE</option>
          </select>
        </div>
        <div class="form-group" style="margin: 0;">
          <label>Destination Path</label>
          <input id="composer-path" class="form-control" type="text" value="/api/webhooks/test" />
        </div>
      </div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px;">
        <div class="form-group" style="margin: 0;">
          <label>Provider (for auto-signing format)</label>
          <select id="composer-provider" class="form-control" style="height: 38px;">
            <option value="generic" selected>Generic</option>
            <option value="stripe">Stripe</option>
            <option value="github">GitHub</option>
            <option value="midtrans">Midtrans</option>
            <option value="xendit">Xendit</option>
            <option value="slack">Slack</option>
            <option value="shopify">Shopify</option>
            <option value="gitlab">GitLab</option>
            <option value="discord">Discord</option>
            <option value="twilio">Twilio</option>
          </select>
        </div>
        <div class="form-group" style="margin: 0;">
          <label>Signing Secret (Optional HMAC key)</label>
          <input id="composer-secret" class="form-control" type="text" placeholder="whsec_... or test secret" />
        </div>
      </div>
      <div class="form-group" style="margin-bottom: 12px;">
        <label>Payload (JSON or Raw Text)</label>
        <textarea id="composer-body" class="form-control" style="height: 140px;">{\n  "event": "order.created",\n  "amount": 150000,\n  "customer": "dev@audienceshift.com"\n}</textarea>
      </div>
      <button class="btn" onclick="dispatchComposer()">🚀 Dispatch to Local Target</button>
      <div id="composer-feedback" style="font-family: var(--font-mono); font-size: 0.85rem; margin-top: 12px;"></div>
    </div>
  `;
}

async function dispatchComposer() {
  const feedback = document.getElementById('composer-feedback');
  if (feedback) feedback.textContent = 'Dispatching custom webhook...';

  const method = document.getElementById('composer-method').value;
  const path = document.getElementById('composer-path').value;
  const provider = document.getElementById('composer-provider').value;
  const secret = document.getElementById('composer-secret').value;
  const body = document.getElementById('composer-body').value;

  try {
    const res = await fetch('/api/composer/dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        method: method,
        path: path,
        provider: provider,
        signing_secret: secret,
        body: body
      })
    });
    const result = await res.json();
    if (feedback) {
      feedback.style.color = result.response.status_code >= 200 && result.response.status_code < 300 ? 'var(--color-success)' : 'var(--color-danger)';
      feedback.textContent = `Dispatched custom webhook -> Local Status ${result.response.status_code} (${result.duration_ms}ms)`;
    }
  } catch (err) {
    if (feedback) {
      feedback.style.color = 'var(--color-danger)';
      feedback.textContent = 'Dispatch failed: ' + err.message;
    }
  }
}

async function renderCatalogTab(container) {
  container.innerHTML = '<div style="color: var(--text-muted);">Loading preset catalog...</div>';
  try {
    const res = await fetch('/api/catalog');
    const presets = await res.json();
    let cards = '';
    presets.forEach(p => {
      cards += `
        <div style="background: var(--bg-secondary); border: 1px solid var(--border-color); border-radius: 8px; padding: 14px; margin-bottom: 12px;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <div style="font-weight: 600;">${escapeHtml(p.name)}</div>
            <span class="pill pill-provider">${escapeHtml(p.provider.toUpperCase())}</span>
          </div>
          <div style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 12px;">${escapeHtml(p.description)}</div>
          <div style="display: flex; gap: 8px;">
            <button class="btn" onclick="triggerPreset('${escapeHtml(p.id)}', false)">Send to Localhost</button>
            <button class="btn btn-secondary" onclick="triggerPreset('${escapeHtml(p.id)}', true)">Send Duplicate (Chaos)</button>
          </div>
        </div>
      `;
    });
    container.innerHTML = `
      <div style="max-width: 800px;">
        <div style="margin-bottom: 16px;">
          <h3 style="font-size: 1.1rem; margin-bottom: 4px;">Universal Webhook Preset Catalog</h3>
          <p style="font-size: 0.85rem; color: var(--text-muted);">Simulate real production webhook events directly to your local endpoint without needing an external test account.</p>
        </div>
        <div class="form-group" style="margin-bottom: 16px;">
          <label>Signing Secret (Optional HMAC key for Stripe, Midtrans, GitHub)</label>
          <input id="catalog-secret" class="form-control" type="text" placeholder="whsec_... or test secret" />
        </div>
        <div id="catalog-feedback" style="font-family: var(--font-mono); font-size: 0.85rem; margin-bottom: 12px;"></div>
        <div>${cards}</div>
      </div>
    `;
  } catch (err) {
    container.innerHTML = '<div style="color: var(--color-danger);">Failed to load catalog: ' + err.message + '</div>';
  }
}

async function triggerPreset(presetId, duplicate) {
  const feedback = document.getElementById('catalog-feedback');
  if (feedback) feedback.textContent = 'Sending preset ' + presetId + '...';
  const secret = document.getElementById('catalog-secret') ? document.getElementById('catalog-secret').value : '';

  try {
    const res = await fetch('/api/catalog/trigger', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        preset_id: presetId,
        signing_secret: secret,
        duplicate: duplicate
      })
    });
    const result = await res.json();
    if (feedback) {
      feedback.style.color = result.response.status_code >= 200 && result.response.status_code < 300 ? 'var(--color-success)' : 'var(--color-danger)';
      feedback.textContent = `Triggered ${presetId} -> Local Status ${result.response.status_code} (${result.duration_ms}ms)`;
    }
  } catch (err) {
    if (feedback) {
      feedback.style.color = 'var(--color-danger)';
      feedback.textContent = 'Trigger error: ' + err.message;
    }
  }
}

async function submitReplay(mutate) {
  if (!selectedEvent) return;
  const feedback = document.getElementById('replay-feedback');
  if (feedback) feedback.textContent = 'Sending replay...';

  const payload = {
    id: selectedEvent.id
  };

  if (mutate) {
    const bodyText = document.getElementById('mutate-body').value;
    const secret = document.getElementById('mutate-secret').value;
    payload.body = btoa(bodyText);
    payload.signing_secret = secret;
  }

  try {
    const res = await fetch('/api/replay', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const result = await res.json();
    if (feedback) {
      feedback.style.color = result.response.status_code >= 200 && result.response.status_code < 300 ? 'var(--color-success)' : 'var(--color-danger)';
      feedback.textContent = `Replayed -> Local Status ${result.response.status_code} (${result.duration_ms}ms)`;
    }

    const diffBox = document.getElementById('replay-diff-box');
    if (diffBox && selectedEvent) {
      let respBodyStr = '';
      try {
        respBodyStr = atob(result.response.body || '');
      } catch {}

      diffBox.innerHTML = `
        <div style="background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 6px; padding: 12px;">
          <div style="font-weight: 600; font-size: 0.85rem; margin-bottom: 8px;">Replay Comparison (Diff)</div>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 8px;">
            <div style="padding: 8px; background: var(--bg-secondary); border-radius: 4px;">
              <div style="font-size: 0.72rem; color: var(--text-muted); font-weight: 600;">ORIGINAL RESPONSE</div>
              <div style="font-family: var(--font-mono); font-size: 0.85rem;">Status: <strong>${selectedEvent.response.status_code}</strong> (${selectedEvent.duration_ms}ms)</div>
            </div>
            <div style="padding: 8px; background: var(--bg-secondary); border-radius: 4px;">
              <div style="font-size: 0.72rem; color: var(--text-muted); font-weight: 600;">REPLAYED RESPONSE</div>
              <div style="font-family: var(--font-mono); font-size: 0.85rem;">Status: <strong style="color: ${result.response.status_code >= 200 && result.response.status_code < 300 ? 'var(--color-success)' : 'var(--color-danger)'}">${result.response.status_code}</strong> (${result.duration_ms}ms)</div>
            </div>
          </div>
          <div style="font-size: 0.72rem; color: var(--text-muted); font-weight: 600; margin-bottom: 4px;">REPLAYED BODY:</div>
          <pre class="code-block" style="max-height: 120px; font-size: 0.78rem; overflow-y: auto;">${escapeHtml(respBodyStr || result.response.error || '(empty response body)')}</pre>
        </div>
      `;
    }
  } catch (err) {
    if (feedback) {
      feedback.style.color = 'var(--color-danger)';
      feedback.textContent = 'Replay request failed: ' + err.message;
    }
  }
}

function exportSession(format) {
  const includeSecrets = confirm("Include unmasked authorization headers in export?");
  window.open(`/api/events/export?format=${format}&include_secrets=${includeSecrets}`, '_blank');
}

function copyAsCurl() {
  if (!selectedEvent) return;
  const localTarget = (document.getElementById('local-target').textContent || 'http://localhost:8080').trim();
  const fullURL = localTarget.replace(/\/$/, '') + (selectedEvent.request.path.startsWith('/') ? '' : '/') + selectedEvent.request.path;

  let curl = `curl -X ${selectedEvent.request.method} "${fullURL}"`;
  for (const [k, v] of Object.entries(selectedEvent.request.headers || {})) {
    const lk = k.toLowerCase();
    if (lk === 'content-length' || lk === 'connection' || lk === 'host' || lk === 'accept-encoding') continue;
    curl += ` -H "${k}: ${v.join(', ')}"`;
  }

  let bodyStr = '';
  if (selectedEvent.request.body) {
    try {
      bodyStr = new TextDecoder().decode(base64ToUint8(selectedEvent.request.body));
    } catch {
      bodyStr = atob(selectedEvent.request.body);
    }
    if (bodyStr) {
      curl += ` -d '${bodyStr.replace(/'/g, "'\\''")}'`;
    }
  }

  navigator.clipboard.writeText(curl).then(() => {
    const btn = document.getElementById('btn-copy-curl');
    if (btn) {
      const orig = btn.textContent;
      btn.textContent = '✅ Copied!';
      setTimeout(() => { btn.textContent = orig; }, 2000);
    }
  }).catch(err => {
    console.error('Clipboard copy failed:', err);
    prompt('Copy cURL command:', curl);
  });
}

async function clearEvents() {
  if (!confirm('Clear all captured events?')) return;
  await fetch('/api/events/clear', { method: 'POST' });
  events = [];
  selectedEvent = null;
  renderEventList();
  renderDetail();
}

function initSearch() {
  document.getElementById('search-input').addEventListener('input', () => {
    renderEventList();
  });
}

function base64ToUint8(base64) {
  if (!base64) return new Uint8Array();
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
}

function isSensitiveHeader(name) {
  const lk = name.toLowerCase();
  return lk.includes('authorization') ||
         lk.includes('cookie') ||
         lk.includes('token') ||
         lk.includes('secret') ||
         lk.includes('api-key') ||
         lk.includes('apikey') ||
         lk.includes('signature');
}

function maskSecretValue(str) {
  if (!str) return '';
  if (str.length <= 8) return '********';
  if (str.toLowerCase().startsWith('bearer ')) {
    const token = str.slice(7);
    if (token.length <= 8) return 'Bearer ********';
    return `Bearer ${token.slice(0, 4)}...${token.slice(-3)}`;
  }
  return `${str.slice(0, 4)}...${str.slice(-4)}`;
}

function toggleHeaderVisibility(idx) {
  const el = document.getElementById(`hdr-val-${idx}`);
  if (!el) return;
  const isMasked = el.getAttribute('data-masked') === 'true';
  const btn = el.nextElementSibling;
  if (isMasked) {
    el.textContent = el.getAttribute('data-original');
    el.setAttribute('data-masked', 'false');
    if (btn) btn.textContent = '🔒 Mask';
  } else {
    el.textContent = el.getAttribute('data-masked-val');
    el.setAttribute('data-masked', 'true');
    if (btn) btn.textContent = '👁️ Reveal';
  }
}


