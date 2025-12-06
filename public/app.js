const bodyEl = document.body;
const EMPTY_DEFAULT_MESSAGE = 'No uploads yet. Click “New List” to process your first file.';
const EMPTY_FILTER_MESSAGE = 'No lists match your search.';
const form = document.getElementById('upload-form');
const fileInput = document.getElementById('file-input');
const userIdInput = document.getElementById('user-id');
const submitBtn = document.getElementById('submit-btn');
const statusBanner = document.getElementById('status-banner');
const resultsSection = document.getElementById('results-section');
const resultsTableBody = document.querySelector('#results-table tbody');
const downloadLink = document.getElementById('download-link');
const jobSummary = document.getElementById('job-summary');
const progressSection = document.getElementById('progress-section');
const progressFill = document.getElementById('progress-fill');
const jobStatusText = document.getElementById('job-status');
const statusCountsList = document.getElementById('status-counts');
const resumeJobBtn = document.getElementById('resume-job');
const newListBtn = document.getElementById('new-list-btn');
const closeUploadBtn = document.getElementById('close-upload');
const jobsTableBody = document.getElementById('jobs-tbody');
const jobSearchInput = document.getElementById('job-search');
const refreshJobsBtn = document.getElementById('refresh-jobs');
const emptyState = document.getElementById('empty-state');

let pollHandle = null;
let activeJobId = null;
let jobCache = [];
let userPinnedUpload = false;

if (form) {
  form.addEventListener('submit', handleUploadSubmit);
}

if (newListBtn) {
  newListBtn.addEventListener('click', () => {
    userPinnedUpload = true;
    bodyEl.classList.add('show-upload');
    resetUploadPanel();
  });
}

if (closeUploadBtn) {
  closeUploadBtn.addEventListener('click', () => {
    userPinnedUpload = false;
    if (jobCache.length) {
      bodyEl.classList.remove('show-upload');
    }
  });
}

if (resumeJobBtn) {
  resumeJobBtn.addEventListener('click', () => {
    const lastJobId = localStorage.getItem('lastJobId');
    if (lastJobId) {
      userPinnedUpload = true;
      bodyEl.classList.add('show-upload');
      startJobTracking(lastJobId);
    }
  });
}

if (jobSearchInput) {
  jobSearchInput.addEventListener('input', applyJobFilter);
}

if (refreshJobsBtn) {
  refreshJobsBtn.addEventListener('click', () => loadJobs());
}

async function handleUploadSubmit(event) {
  event.preventDefault();
  if (!fileInput?.files?.length) {
    showStatus('error', 'Please select a CSV or Excel file to upload.');
    return;
  }

  toggleLoading(true);
  showStatus('neutral', 'Uploading file and starting the job...');
  const userId = (userIdInput?.value || '').trim() || 'demo-user';
  const formData = new FormData();
  formData.append('file', fileInput.files[0]);

  try {
    const response = await fetch('/v1/scraper/enricher/upload', {
      method: 'POST',
      headers: { 'x-user-id': userId },
      body: formData,
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || 'Upload failed');
    }
    if (payload?.results?.length) {
      renderResults(payload);
    }
    startJobTracking(payload.jobId);
    const completed = Boolean(payload?.results?.length) || payload.status === 'completed';
    const message = completed
      ? `Job ${payload.jobId} completed successfully.`
      : `Job ${payload.jobId} started. Track progress from the dashboard.`;
    showStatus('success', message);
    userPinnedUpload = false;
    await loadJobs({ silent: true });
    bodyEl.classList.remove('show-upload');
  } catch (error) {
    console.error(error);
    showStatus('error', error.message);
  } finally {
    toggleLoading(false);
  }
}

function renderResults(payload) {
  if (!payload?.results?.length) {
    showStatus('error', 'No contacts found after parsing the file.');
    return;
  }
  if (resultsSection) {
    resultsSection.hidden = false;
  }
  if (downloadLink) {
    downloadLink.href = payload.downloadUrl;
  }
  if (jobSummary) {
    jobSummary.textContent = `${payload.results.length} contacts processed. Output saved as ${payload.outputFile}.`;
  }

  if (!resultsTableBody) {
    return;
  }

  resultsTableBody.innerHTML = '';
  payload.results.forEach((result) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${result.firstName || ''}</td>
      <td>${result.lastName || ''}</td>
      <td>${result.domain || ''}</td>
      <td>${result.bestEmail || '<span class="muted">n/a</span>'}</td>
      <td>${result.status || ''}</td>
      <td>${deriveMessage(result)}</td>
    `;
    resultsTableBody.appendChild(row);
  });
}

function resetUploadPanel() {
  form?.reset();
  clearStatus();
  if (resultsSection) {
    resultsSection.hidden = true;
  }
  if (progressSection) {
    progressSection.hidden = true;
  }
  if (resultsTableBody) {
    resultsTableBody.innerHTML = '';
  }
  if (downloadLink) {
    downloadLink.removeAttribute('href');
  }
  if (jobSummary) {
    jobSummary.textContent = '';
  }
}

function startJobTracking(jobId) {
  if (!jobId) {
    return;
  }
  activeJobId = jobId;
  localStorage.setItem('lastJobId', jobId);
  if (progressSection) {
    progressSection.hidden = false;
  }
  if (jobStatusText) {
    jobStatusText.textContent = `Job ${jobId} • processing`;
  }
  if (resumeJobBtn) {
    resumeJobBtn.hidden = false;
  }
  fetchJobStatus(jobId);
  if (pollHandle) {
    clearInterval(pollHandle);
  }
  pollHandle = setInterval(() => fetchJobStatus(jobId), 4000);
}

async function fetchJobStatus(jobId, { silent } = {}) {
  try {
    const response = await fetch(`/v1/scraper/enricher/jobs/${jobId}`);
    if (!response.ok) {
      if (!silent) {
        showStatus('error', 'Unable to load job status.');
      }
      cleanupJobPolling();
      localStorage.removeItem('lastJobId');
      if (resumeJobBtn) {
        resumeJobBtn.hidden = true;
      }
      return null;
    }
    const metadata = await response.json();
    updateProgressUI(metadata);
    if (metadata.status === 'completed' || metadata.status === 'failed') {
      cleanupJobPolling();
      localStorage.removeItem('lastJobId');
      if (resumeJobBtn) {
        resumeJobBtn.hidden = true;
      }
      await loadJobs({ silent: true });
    }
    return metadata;
  } catch (error) {
    if (!silent) {
      showStatus('error', error.message);
    }
    return null;
  }
}

function cleanupJobPolling() {
  if (pollHandle) {
    clearInterval(pollHandle);
    pollHandle = null;
  }
  activeJobId = null;
}

function updateProgressUI(metadata) {
  if (!metadata) {
    return;
  }
  const { jobId, status, progress = {}, downloadUrl } = metadata;
  if (jobStatusText) {
    jobStatusText.textContent = `Job ${jobId} • ${status}`;
  }
  const percent = progress.totalContacts
    ? Math.round((progress.processedContacts / progress.totalContacts) * 100)
    : status === 'completed' ? 100 : 0;
  if (progressFill) {
    progressFill.style.width = `${Math.min(100, percent)}%`;
  }

  const counts = progress.statusCounts || {};
  if (statusCountsList) {
    statusCountsList.innerHTML = '';
    const displayOrder = ['valid', 'catchall_default', 'not_found_valid_emails', 'error', 'other', 'skipped'];
    const labelMap = {
      valid: 'Valid',
      catchall_default: 'Catch-All',
      not_found_valid_emails: 'Not Found',
      error: 'Error',
      other: 'Other',
      skipped: 'Skipped',
    };

    displayOrder.forEach((key) => {
      const value = counts[key] ?? 0;
      const item = document.createElement('li');
      item.innerHTML = `<span>${labelMap[key]}</span><strong>${value}</strong>`;
      statusCountsList.appendChild(item);
    });

    Object.entries(counts).forEach(([key, value]) => {
      if (displayOrder.includes(key)) {
        return;
      }
      const label = labelMap[key] || key;
      const item = document.createElement('li');
      item.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
      statusCountsList.appendChild(item);
    });
  }

  if (downloadUrl && downloadLink) {
    downloadLink.href = downloadUrl;
  }
}

async function loadJobs({ silent } = {}) {
  try {
    const response = await fetch('/v1/scraper/enricher/jobs');
    if (!response.ok) {
      throw new Error('Unable to load jobs');
    }
    const { jobs = [] } = await response.json();
    jobCache = jobs;
    applyJobFilter();
    ensureDefaultView();
  } catch (error) {
    if (!silent) {
      showStatus('error', error.message);
    }
  }
}

function ensureDefaultView() {
  if (!jobCache.length) {
    bodyEl.classList.add('show-upload');
    if (closeUploadBtn) {
      closeUploadBtn.hidden = true;
    }
    if (emptyState) {
      emptyState.style.display = 'block';
    }
    return;
  }

  if (closeUploadBtn) {
    closeUploadBtn.hidden = false;
  }
  if (!userPinnedUpload) {
    bodyEl.classList.remove('show-upload');
  }
}

function applyJobFilter() {
  if (!jobsTableBody) {
    return;
  }
  const term = (jobSearchInput?.value || '').trim().toLowerCase();
  const subset = term
    ? jobCache.filter((job) => (job.originalFilename || '').toLowerCase().includes(term))
    : jobCache;
  renderJobs(subset);
}

function renderJobs(jobs) {
  if (!jobsTableBody) {
    return;
  }
  jobsTableBody.innerHTML = '';
  if (!jobs.length) {
    if (emptyState) {
      emptyState.style.display = 'block';
      emptyState.textContent = jobCache.length ? EMPTY_FILTER_MESSAGE : EMPTY_DEFAULT_MESSAGE;
    }
    return;
  }
  if (emptyState) {
    emptyState.style.display = 'none';
    emptyState.textContent = EMPTY_DEFAULT_MESSAGE;
  }

  jobs.forEach((job) => {
    const total = job.totals?.totalRows ?? job.progress?.totalContacts ?? 0;
    const enriched = job.resultCount ?? job.progress?.processedContacts ?? 0;
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>
        <div class="file-cell">
          <strong>${job.originalFilename || 'Untitled file'}</strong>
          <span class="meta">${formatJobDate(job.createdAt)}</span>
        </div>
      </td>
      <td>${renderStatusPill(job.status)}</td>
      <td>${total}</td>
      <td>${enriched}</td>
      <td>${renderDownloadCell(job)}</td>
    `;
    jobsTableBody.appendChild(row);
  });
}

function renderStatusPill(status = 'pending') {
  const normalized = status || 'pending';
  const label = formatStatusLabel(normalized);
  return `<span class="status-pill ${normalized}">${label}</span>`;
}

function formatStatusLabel(status) {
  const map = {
    completed: 'Done',
    processing: 'Processing',
    failed: 'Failed',
  };
  return map[status] || 'Pending';
}

function renderDownloadCell(job) {
  if (job.downloadUrl) {
    return `<a class="download-link" href="${job.downloadUrl}" target="_blank" rel="noopener" download>Download</a>`;
  }
  return '<span class="muted">Pending</span>';
}

function formatJobDate(value) {
  if (!value) {
    return '—';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '—';
  }
  return date.toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function deriveMessage(result) {
  if (!result?.details) {
    return '';
  }
  return result.details.message || result.details.reason || '';
}

function showStatus(type, message) {
  if (!statusBanner) {
    return;
  }
  statusBanner.textContent = message;
  statusBanner.classList.remove('success', 'error', 'neutral', 'visible');
  if (message) {
    if (type && type !== 'neutral') {
      statusBanner.classList.add(type);
    }
    statusBanner.classList.add('visible');
  }
}

function clearStatus() {
  if (!statusBanner) {
    return;
  }
  statusBanner.textContent = '';
  statusBanner.classList.remove('success', 'error', 'neutral', 'visible');
}

function toggleLoading(isLoading) {
  if (!submitBtn) {
    return;
  }
  submitBtn.disabled = isLoading;
  submitBtn.textContent = isLoading ? 'Processing…' : 'Run Enrichment';
}

function restoreLastJob() {
  const lastJobId = localStorage.getItem('lastJobId');
  if (!lastJobId) {
    return;
  }
  if (resumeJobBtn) {
    resumeJobBtn.hidden = false;
  }
  fetchJobStatus(lastJobId, { silent: true }).then((metadata) => {
    if (metadata) {
      startJobTracking(lastJobId);
    } else if (resumeJobBtn) {
      resumeJobBtn.hidden = true;
      localStorage.removeItem('lastJobId');
    }
  });
}

loadJobs();
restoreLastJob();
