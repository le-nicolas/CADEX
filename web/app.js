const ui = {
  configText: document.getElementById("configText"),
  casesText: document.getElementById("casesText"),
  logs: document.getElementById("logs"),
  introspection: document.getElementById("introspection"),
  runState: document.getElementById("runState"),
  runMode: document.getElementById("runMode"),
  caseCounter: document.getElementById("caseCounter"),
  currentCase: document.getElementById("currentCase"),
  liveFailureWrap: document.getElementById("liveFailureWrap"),
  resultsLinks: document.getElementById("resultsLinks"),
  resultsTableWrap: document.getElementById("resultsTableWrap"),
  failureWrap: document.getElementById("failureWrap"),
  chartsWrap: document.getElementById("chartsWrap"),
  reloadBtn: document.getElementById("reloadBtn"),
  saveConfigBtn: document.getElementById("saveConfigBtn"),
  saveCasesBtn: document.getElementById("saveCasesBtn"),
  introspectBtn: document.getElementById("introspectBtn"),
  runAllBtn: document.getElementById("runAllBtn"),
  runFailedBtn: document.getElementById("runFailedBtn"),
  runChangedBtn: document.getElementById("runChangedBtn"),
  llmPrompt: document.getElementById("llmPrompt"),
  llmMaxRows: document.getElementById("llmMaxRows"),
  llmPreviewBtn: document.getElementById("llmPreviewBtn"),
  llmApplyBtn: document.getElementById("llmApplyBtn"),
  llmResult: document.getElementById("llmResult"),
  meshPrompt: document.getElementById("meshPrompt"),
  meshSuggestBtn: document.getElementById("meshSuggestBtn"),
  meshApplyBtn: document.getElementById("meshApplyBtn"),
  meshResult: document.getElementById("meshResult"),
  loopObjectiveAlias: document.getElementById("loopObjectiveAlias"),
  loopObjectiveGoal: document.getElementById("loopObjectiveGoal"),
  loopBatchSize: document.getElementById("loopBatchSize"),
  loopMaxBatches: document.getElementById("loopMaxBatches"),
  loopSearchSpace: document.getElementById("loopSearchSpace"),
  loopConstraints: document.getElementById("loopConstraints"),
  loopFixedValues: document.getElementById("loopFixedValues"),
  loopStartBtn: document.getElementById("loopStartBtn"),
  loopStopBtn: document.getElementById("loopStopBtn"),
  loopPreflight: document.getElementById("loopPreflight"),
  loopConvergenceChart: document.getElementById("loopConvergenceChart"),
  loopTimelineWrap: document.getElementById("loopTimelineWrap"),
  loopBatchTableWrap: document.getElementById("loopBatchTableWrap"),
  loopStatus: document.getElementById("loopStatus"),
  studyPathInput: document.getElementById("studyPathInput"),
  discoverStudiesBtn: document.getElementById("discoverStudiesBtn"),
  applyStudyPathBtn: document.getElementById("applyStudyPathBtn"),
  studyCandidates: document.getElementById("studyCandidates"),
  useSelectedStudyBtn: document.getElementById("useSelectedStudyBtn"),
  solveBanner: document.getElementById("solveBanner"),
  authBanner: document.getElementById("authBanner"),
  apiKeyInput: document.getElementById("apiKeyInput"),
  surrogateObjectiveAlias: document.getElementById("surrogateObjectiveAlias"),
  surrogateObjectiveGoal: document.getElementById("surrogateObjectiveGoal"),
  surrogateSampleCount: document.getElementById("surrogateSampleCount"),
  surrogateTopN: document.getElementById("surrogateTopN"),
  surrogateValidateTopN: document.getElementById("surrogateValidateTopN"),
  surrogateMinRows: document.getElementById("surrogateMinRows"),
  surrogateSearchSpace: document.getElementById("surrogateSearchSpace"),
  surrogateConstraints: document.getElementById("surrogateConstraints"),
  surrogateFixedValues: document.getElementById("surrogateFixedValues"),
  surrogateTrainBtn: document.getElementById("surrogateTrainBtn"),
  surrogateRefreshBtn: document.getElementById("surrogateRefreshBtn"),
  surrogatePredictBtn: document.getElementById("surrogatePredictBtn"),
  surrogateValidateBtn: document.getElementById("surrogateValidateBtn"),
  surrogateWorkflow: document.getElementById("surrogateWorkflow"),
  surrogateFeatureBars: document.getElementById("surrogateFeatureBars"),
  surrogateStatus: document.getElementById("surrogateStatus"),
  surrogatePredictOutput: document.getElementById("surrogatePredictOutput"),
  surrogateCoverage: document.getElementById("surrogateCoverage"),
  casesMatrixWrap: document.getElementById("casesMatrixWrap"),
  livePhaseWrap: document.getElementById("livePhaseWrap"),
  liveCaseTableWrap: document.getElementById("liveCaseTableWrap"),
  bestDesignCard: document.getElementById("bestDesignCard"),
  failureBreakdownWrap: document.getElementById("failureBreakdownWrap"),
  historyStudyFilter: document.getElementById("historyStudyFilter"),
  historyCaseFilter: document.getElementById("historyCaseFilter"),
  historyRefreshBtn: document.getElementById("historyRefreshBtn"),
  historyMeta: document.getElementById("historyMeta"),
  historyQuickPicks: document.getElementById("historyQuickPicks"),
  historyRunsWrap: document.getElementById("historyRunsWrap"),
  historyCases: document.getElementById("historyCases"),
};

let currentConfig = null;
let historySelectedRunId = "";
let latestRunStatus = {};
let latestDesignLoopStatus = {};
let latestRunSummary = {};
let surrogatePredictState = {
  predictedAt: "",
  validating: false,
};

function getApiKey() {
  const key = (ui.apiKeyInput.value || localStorage.getItem("cfd_api_key") || "").trim();
  return key;
}

function persistApiKey() {
  const key = getApiKey();
  if (key) {
    localStorage.setItem("cfd_api_key", key);
  } else {
    localStorage.removeItem("cfd_api_key");
  }
}

async function callApi(url, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.method && options.method !== "GET" && options.method !== "HEAD") {
    headers["Content-Type"] = "application/json";
  }
  const apiKey = getApiKey();
  if (apiKey) {
    headers["X-API-Key"] = apiKey;
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || `Request failed: ${url}`);
  }
  return payload;
}

function flash(message) {
  const stamp = new Date().toISOString();
  ui.logs.textContent = `[${stamp}] ${message}\n` + ui.logs.textContent;
}

function updateSolveBanner() {
  const solveEnabled = Boolean(currentConfig && currentConfig.solve && currentConfig.solve.enabled);
  ui.solveBanner.classList.toggle("hidden", solveEnabled);
}

function updateStatusView(status) {
  latestRunStatus = status || {};
  const running = Boolean(status.running);
  ui.runState.textContent = running ? "Running" : "Idle";
  ui.runState.className = `pill ${running ? "running" : "idle"}`;
  ui.runMode.textContent = `Mode: ${status.mode || "-"}`;
  ui.caseCounter.textContent = `${status.completed_case_count || 0} / ${status.selected_case_count || 0}`;
  const currentPhase = status.current_phase || "startup";
  ui.currentCase.textContent = `Current: ${status.current_case || "-"} (${currentPhase})`;

  const logLines = (status.logs || []).slice().reverse();
  if (status.last_error) {
    logLines.unshift(`[ERROR] ${status.last_error}`);
  }
  ui.logs.textContent = logLines.join("\n");

  ui.authBanner.classList.toggle("hidden", !status.auth_required);
  if (!(status.running && status.mode === "validate")) {
    surrogatePredictState.validating = false;
  }
  renderPhasePipeline(status.current_phase || "startup");
  renderLiveCaseTable(status.case_table || []);
  renderLiveFailures(status.recent_failures || []);
}

function normalizeFailureType(value) {
  return String(value || "").trim().toLowerCase().replace(/[^a-z0-9_]+/g, "_");
}

function renderPhasePipeline(activePhase) {
  const phases = ["mesh", "solve", "extract"];
  const normalized = String(activePhase || "").trim().toLowerCase();
  const mapPhase = normalized === "results" ? "extract" : normalized;
  const idx = phases.indexOf(mapPhase);

  ui.livePhaseWrap.innerHTML = "";
  const wrap = document.createElement("div");
  wrap.className = "phase-pipeline";
  phases.forEach((phase, phaseIdx) => {
    const chip = document.createElement("span");
    let statusClass = "pending";
    if (idx >= 0 && phaseIdx < idx) statusClass = "done";
    else if (phaseIdx === idx) statusClass = "active";
    chip.className = `phase-chip ${statusClass}`;
    chip.textContent = phase;
    wrap.appendChild(chip);
  });
  ui.livePhaseWrap.appendChild(wrap);
}

function renderLiveCaseTable(rows) {
  const items = Array.isArray(rows) ? rows : [];
  ui.liveCaseTableWrap.innerHTML = "";
  if (!items.length) return;

  const sorted = items.slice().sort((a, b) => String(a.case_id || "").localeCompare(String(b.case_id || "")));
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  ["case_id", "status", "phase", "attempt", "failure_type"].forEach((name) => {
    const th = document.createElement("th");
    th.textContent = name;
    trHead.appendChild(th);
  });
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  sorted.slice(0, 50).forEach((row) => {
    const tr = document.createElement("tr");
    const cells = [
      String(row.case_id || ""),
      String(row.status || ""),
      String(row.phase || ""),
      String(row.attempt || ""),
      String(row.failure_type || ""),
    ];
    cells.forEach((value, idx) => {
      const td = document.createElement("td");
      if (idx === 4 && value) {
        const tag = document.createElement("span");
        tag.className = `failure-tag failure-${normalizeFailureType(value)}`;
        tag.textContent = value;
        td.appendChild(tag);
      } else {
        td.textContent = value;
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  ui.liveCaseTableWrap.appendChild(table);
}

function renderLiveFailures(items) {
  ui.liveFailureWrap.innerHTML = "";
  if (!items.length) return;
  const latest = items.slice(-3).reverse();
  for (const item of latest) {
    const div = document.createElement("div");
    const normalized = normalizeFailureType(item.failure_type || "unknown");
    div.className = `failure-item failure-${normalized}`;
    const typeText = item.failure_type ? `[${item.failure_type}] ` : "";
    const modeText = item.failure_mode ? ` (${item.failure_mode})` : "";
    div.textContent = `${item.case_id} (attempt ${item.attempt}) ${typeText}${item.reason || "Unknown failure"}${modeText}`;
    ui.liveFailureWrap.appendChild(div);
  }
}

function renderLinks(summary) {
  ui.resultsLinks.innerHTML = "";
  const links = [];
  const results = summary.results || {};

  if (results.master_csv_url) links.push(["Master CSV", results.master_csv_url]);
  if (results.ranked_csv_url) links.push(["Ranked CSV", results.ranked_csv_url]);
  if (results.report_md_url) links.push(["Report MD", results.report_md_url]);
  if (results.report_html_url) links.push(["Report HTML", results.report_html_url]);

  for (const [label, href] of links) {
    const a = document.createElement("a");
    a.href = href;
    a.target = "_blank";
    a.rel = "noreferrer";
    a.textContent = label;
    ui.resultsLinks.appendChild(a);
  }

  const rows = Array.isArray(summary.case_results) ? summary.case_results : [];
  const caseAssets = rows
    .map((row) => {
      const screenshots = Array.isArray(row.screenshot_urls)
        ? row.screenshot_urls.filter((url) => Boolean(url))
        : [];
      const linksForCase = [];
      if (row.summary_csv_url) linksForCase.push(["Summary CSV", row.summary_csv_url]);
      if (row.metrics_csv_url) linksForCase.push(["Metrics CSV", row.metrics_csv_url]);
      screenshots.forEach((url, idx) => linksForCase.push([`Screenshot ${idx + 1}`, url]));
      return {
        caseId: String(row.case_id || ""),
        links: linksForCase,
      };
    })
    .filter((item) => item.links.length > 0);

  if (!caseAssets.length) {
    return;
  }

  const heading = document.createElement("div");
  heading.className = "helper";
  heading.textContent = "Per-case output files:";
  ui.resultsLinks.appendChild(heading);

  for (const item of caseAssets) {
    const rowWrap = document.createElement("div");
    rowWrap.className = "helper";
    rowWrap.appendChild(document.createTextNode(`${item.caseId || "case"}: `));
    item.links.forEach(([label, href], idx) => {
      const a = document.createElement("a");
      a.href = href;
      a.target = "_blank";
      a.rel = "noreferrer";
      a.textContent = label;
      rowWrap.appendChild(a);
      if (idx < item.links.length - 1) {
        rowWrap.appendChild(document.createTextNode(" | "));
      }
    });
    ui.resultsLinks.appendChild(rowWrap);
  }
}

function toNumber(value) {
  const num = Number.parseFloat(String(value ?? "").trim());
  return Number.isFinite(num) ? num : null;
}

function resolveObjectiveSpec(summary) {
  const ranking = Array.isArray(currentConfig && currentConfig.ranking) ? currentConfig.ranking : [];
  if (ranking.length && ranking[0] && ranking[0].alias) {
    return {
      alias: String(ranking[0].alias),
      goal: String(ranking[0].goal || "min").toLowerCase() === "max" ? "max" : "min",
    };
  }
  const loopSummary = (latestDesignLoopStatus && latestDesignLoopStatus.last_summary) || {};
  if (loopSummary.objective_alias) {
    return {
      alias: String(loopSummary.objective_alias),
      goal: String(loopSummary.objective_goal || "min").toLowerCase() === "max" ? "max" : "min",
    };
  }
  const rows = Array.isArray(summary.case_results) ? summary.case_results : [];
  for (const row of rows) {
    const metrics = row.metrics || {};
    const keys = Object.keys(metrics);
    if (keys.length) {
      return { alias: keys[0], goal: "min" };
    }
  }
  return { alias: "", goal: "min" };
}

function buildResultRows(summary, objectiveSpec) {
  const rows = Array.isArray(summary.case_results) ? summary.case_results : [];
  return rows.map((row) => {
    const metrics = row.metrics || {};
    const objectiveValue = objectiveSpec.alias ? toNumber(metrics[objectiveSpec.alias]) : null;
    return {
      ...row,
      objective_value: objectiveValue,
    };
  });
}

function sortByObjective(rows, objectiveSpec) {
  const list = rows.slice();
  list.sort((a, b) => {
    if (Boolean(a.success) !== Boolean(b.success)) return a.success ? -1 : 1;
    const aObj = toNumber(a.objective_value);
    const bObj = toNumber(b.objective_value);
    if (aObj === null && bObj !== null) return 1;
    if (aObj !== null && bObj === null) return -1;
    if (aObj !== null && bObj !== null) {
      return objectiveSpec.goal === "max" ? bObj - aObj : aObj - bObj;
    }
    return String(a.case_id || "").localeCompare(String(b.case_id || ""));
  });
  return list;
}

function renderScoreBar(normalized) {
  const barWrap = document.createElement("div");
  barWrap.className = "score-bar";
  const fill = document.createElement("div");
  fill.className = "score-fill";
  fill.style.width = `${Math.max(0, Math.min(100, normalized * 100)).toFixed(1)}%`;
  barWrap.appendChild(fill);
  return barWrap;
}

function renderBestDesignCard(summary, objectiveSpec, sortedRows) {
  ui.bestDesignCard.innerHTML = "";
  const card = document.createElement("div");
  card.className = "best-card";

  const loopSummary = (latestDesignLoopStatus && latestDesignLoopStatus.last_summary) || {};
  const loopBest = loopSummary.best_case && loopSummary.best_case.case_id ? loopSummary.best_case : null;
  const topResult = sortedRows.find((row) => row.success);
  const bestCaseId = loopBest ? loopBest.case_id : (topResult ? topResult.case_id : "-");
  const bestObjective = loopBest && loopBest.objective_value !== undefined
    ? loopBest.objective_value
    : (topResult ? topResult.objective_value : null);

  const title = document.createElement("h3");
  title.textContent = "Best Design";
  card.appendChild(title);

  const line = document.createElement("div");
  line.className = "helper";
  const objectiveText = objectiveSpec.alias
    ? `${objectiveSpec.alias}=${bestObjective ?? "-"}`
    : "objective not configured";
  line.textContent = `${bestCaseId} | ${objectiveText}`;
  card.appendChild(line);

  let explanation = "";
  const history = Array.isArray(loopSummary.history) ? loopSummary.history : [];
  if (history.length) {
    const latestBatch = history[history.length - 1];
    const narration = latestBatch && latestBatch.narration ? latestBatch.narration : {};
    explanation = String(narration.text || "").trim();
  }
  if (!explanation) {
    explanation = "No LLM explanation available for this run yet.";
  }
  const block = document.createElement("blockquote");
  block.className = "explanation";
  block.textContent = explanation;
  card.appendChild(block);
  ui.bestDesignCard.appendChild(card);
}

function renderFailureBreakdown(summary) {
  ui.failureBreakdownWrap.innerHTML = "";
  const counts = new Map();
  const addFailure = (type) => {
    const key = normalizeFailureType(type || "unknown");
    counts.set(key, (counts.get(key) || 0) + 1);
  };

  const runRows = Array.isArray(summary.case_results) ? summary.case_results : [];
  runRows.forEach((row) => {
    if (!row.success) addFailure(row.failure_type || "unknown");
  });

  const loopSummary = (latestDesignLoopStatus && latestDesignLoopStatus.last_summary) || {};
  const history = Array.isArray(loopSummary.history) ? loopSummary.history : [];
  history.forEach((batch) => {
    const cases = Array.isArray(batch.cases) ? batch.cases : [];
    cases.forEach((item) => {
      if (!item.success) addFailure(item.failure_type || "unknown");
    });
  });

  if (!counts.size) {
    ui.failureBreakdownWrap.textContent = "Failure summary: no failed cases.";
    return;
  }

  const title = document.createElement("h3");
  title.textContent = "Failure Summary";
  ui.failureBreakdownWrap.appendChild(title);

  const wrap = document.createElement("div");
  wrap.className = "failure-breakdown";
  Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .forEach(([type, count]) => {
      const pill = document.createElement("span");
      pill.className = `failure-tag failure-${type}`;
      pill.textContent = `${type}: ${count}`;
      wrap.appendChild(pill);
    });
  ui.failureBreakdownWrap.appendChild(wrap);
}

function renderResultsTable(summary) {
  const objectiveSpec = resolveObjectiveSpec(summary);
  const rows = sortByObjective(buildResultRows(summary, objectiveSpec), objectiveSpec);
  ui.resultsTableWrap.innerHTML = "";
  if (!rows.length) {
    ui.resultsTableWrap.textContent = "No run results yet.";
    ui.bestDesignCard.textContent = "";
    return;
  }

  const numericValues = rows
    .filter((row) => row.success && row.objective_value !== null)
    .map((row) => Number(row.objective_value));
  const min = numericValues.length ? Math.min(...numericValues) : 0;
  const max = numericValues.length ? Math.max(...numericValues) : 0;
  const range = max - min;

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  ["rank", "case_id", "status", objectiveSpec.alias || "objective", "score", "failure"].forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    trHead.appendChild(th);
  });
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row, index) => {
    const tr = document.createElement("tr");

    const tdRank = document.createElement("td");
    tdRank.textContent = String(index + 1);
    tr.appendChild(tdRank);

    const tdCase = document.createElement("td");
    tdCase.textContent = String(row.case_id || "");
    tr.appendChild(tdCase);

    const tdStatus = document.createElement("td");
    const statusTag = document.createElement("span");
    statusTag.className = `tag ${row.success ? "success" : "failed"}`;
    statusTag.textContent = row.success ? "success" : "failed";
    tdStatus.appendChild(statusTag);
    tr.appendChild(tdStatus);

    const tdObjective = document.createElement("td");
    tdObjective.textContent = row.objective_value !== null ? String(row.objective_value) : "-";
    tr.appendChild(tdObjective);

    const tdScore = document.createElement("td");
    if (row.objective_value !== null && (range > 0 || numericValues.length === 1)) {
      const normalized = numericValues.length === 1
        ? 1.0
        : objectiveSpec.goal === "max"
          ? (Number(row.objective_value) - min) / range
          : (max - Number(row.objective_value)) / range;
      tdScore.appendChild(renderScoreBar(normalized));
    } else {
      tdScore.textContent = "-";
    }
    tr.appendChild(tdScore);

    const tdFailure = document.createElement("td");
    if (!row.success) {
      const type = normalizeFailureType(row.failure_type || "unknown");
      const failureTag = document.createElement("span");
      failureTag.className = `failure-tag failure-${type}`;
      failureTag.textContent = row.failure_type || "unknown";
      tdFailure.appendChild(failureTag);
      const reason = document.createElement("div");
      reason.textContent = shortText(row.failure_reason || row.error || "", 120);
      tdFailure.appendChild(reason);
    } else {
      tdFailure.textContent = "";
    }
    tr.appendChild(tdFailure);
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  ui.resultsTableWrap.appendChild(table);
  renderBestDesignCard(summary, objectiveSpec, rows);
}

function renderFailureDetails(summary) {
  ui.failureWrap.innerHTML = "";
  const rows = Array.isArray(summary.case_results) ? summary.case_results : [];
  const failed = rows.filter((row) => !row.success);
  if (!failed.length) {
    return;
  }
  const title = document.createElement("h3");
  title.textContent = "Failure Reasons";
  ui.failureWrap.appendChild(title);

  failed.forEach((row) => {
    const type = normalizeFailureType(row.failure_type || "unknown");
    const div = document.createElement("div");
    div.className = `failure-item failure-${type}`;
    const reason = row.failure_reason || row.error || "Unknown failure";
    const typeText = row.failure_type ? ` [${row.failure_type}]` : "";
    div.textContent = `${row.case_id}${typeText}: ${reason}`;
    ui.failureWrap.appendChild(div);
  });
}

function renderCharts(summary) {
  ui.chartsWrap.innerHTML = "";
  const chartUrls = (summary.results && summary.results.chart_urls) || [];
  for (const chartUrl of chartUrls) {
    if (!chartUrl) continue;
    const img = document.createElement("img");
    img.src = chartUrl;
    img.loading = "lazy";
    ui.chartsWrap.appendChild(img);
  }
}

function renderSummary(summary) {
  renderLinks(summary);
  renderResultsTable(summary);
  renderFailureBreakdown(summary);
  renderFailureDetails(summary);
  renderCharts(summary);
}

function shortText(value, maxLen = 90) {
  const text = String(value || "").trim();
  if (!text) return "-";
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen - 3)}...`;
}

function formatUtc(value) {
  const text = String(value || "").trim();
  if (!text) return "-";
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return text;
  return parsed.toLocaleString();
}

function renderHistoryRuns(payload) {
  const rows = Array.isArray(payload.runs) ? payload.runs : [];
  ui.historyMeta.textContent = `Stored runs: ${payload.total || 0}`;
  ui.historyQuickPicks.innerHTML = "";
  ui.historyRunsWrap.innerHTML = "";
  if (!rows.length) {
    ui.historyRunsWrap.textContent = "No run history found for this filter.";
    return;
  }

  const quick = document.createElement("div");
  quick.className = "quick-picks";

  const addQuickLink = (label, runId) => {
    if (!runId) return;
    const btn = document.createElement("button");
    btn.textContent = label;
    btn.addEventListener("click", async () => {
      try {
        await loadHistoryRun(runId);
      } catch (err) {
        flash(`Load history run failed: ${err.message}`);
      }
    });
    quick.appendChild(btn);
  };

  const first = rows[0];
  addQuickLink(`Latest (${formatUtc(first.created_at)})`, first.run_id);
  const kani = rows.find((row) => String(row.study_path || "").toLowerCase().includes("kani yawa"));
  if (kani) {
    addQuickLink(`Kani Yawa (${formatUtc(kani.created_at)})`, kani.run_id);
  }
  ui.historyQuickPicks.appendChild(quick);

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  ["date", "run_id", "mode", "study_path", "success/failed", ""].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.className = "click-row";
    if (historySelectedRunId === row.run_id) {
      tr.classList.add("selected-row");
    }
    if (String(row.study_path || "").toLowerCase().includes("kani yawa")) {
      tr.classList.add("kani-row");
    }

    tr.addEventListener("click", async () => {
      try {
        await loadHistoryRun(row.run_id);
      } catch (err) {
        flash(`Load history run failed: ${err.message}`);
      }
    });

    const tdDate = document.createElement("td");
    tdDate.textContent = formatUtc(row.created_at);
    tr.appendChild(tdDate);

    const tdRun = document.createElement("td");
    tdRun.textContent = row.run_id || "";
    tr.appendChild(tdRun);

    const tdMode = document.createElement("td");
    tdMode.textContent = row.mode || "-";
    tr.appendChild(tdMode);

    const tdStudy = document.createElement("td");
    tdStudy.textContent = shortText(row.study_path || "", 88);
    tdStudy.title = row.study_path || "";
    tr.appendChild(tdStudy);

    const tdScore = document.createElement("td");
    tdScore.textContent = `${row.successful_cases || 0}/${row.failed_cases || 0}`;
    tr.appendChild(tdScore);

    const tdOpen = document.createElement("td");
    const btn = document.createElement("button");
    btn.textContent = "Load";
    btn.className = historySelectedRunId === row.run_id ? "accent" : "";
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      try {
        await loadHistoryRun(row.run_id);
      } catch (err) {
        flash(`Load history run failed: ${err.message}`);
      }
    });
    tdOpen.appendChild(btn);
    tr.appendChild(tdOpen);

    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  ui.historyRunsWrap.appendChild(table);
}

function renderHistoryCases(payload) {
  const rows = Array.isArray(payload.cases) ? payload.cases : [];
  if (!rows.length) {
    ui.historyCases.textContent = "No case records found for this filter.";
    return;
  }
  const lines = [];
  lines.push(`Case records: ${payload.total || 0}`);
  lines.push("");
  for (const row of rows.slice(0, 40)) {
    const metrics = row.metrics || {};
    lines.push(
      `[${row.run_id}] ${row.case_id} :: ${row.success ? "success" : "failed"} ` +
      `${row.failure_type ? `(${row.failure_type})` : ""}`
    );
    const metricKeys = Object.keys(metrics);
    if (metricKeys.length) {
      const metricPreview = metricKeys
        .slice(0, 4)
        .map((key) => `${key}=${metrics[key]}`)
        .join(", ");
      lines.push(`  metrics: ${metricPreview}`);
    }
    if (!row.success && row.failure_reason) {
      lines.push(`  reason: ${shortText(row.failure_reason, 180)}`);
    }
  }
  if (rows.length > 40) {
    lines.push("");
    lines.push(`Showing first 40 records from ${rows.length} loaded.`);
  }
  ui.historyCases.textContent = lines.join("\n");
}

function highlightSelectedHistoryRun() {
  const rows = ui.historyRunsWrap.querySelectorAll("tbody tr");
  rows.forEach((row) => {
    const runCell = row.querySelector("td:nth-child(2)");
    const runId = runCell ? runCell.textContent : "";
    row.classList.toggle("selected-row", runId === historySelectedRunId);
  });
}

async function loadHistoryRun(runId) {
  const payload = await callApi(`/api/history/runs/${encodeURIComponent(runId)}`);
  historySelectedRunId = runId;
  highlightSelectedHistoryRun();
  latestRunSummary = payload.summary || {};
  renderSummary(latestRunSummary);
  flash(`Loaded historical run ${runId}.`);
}

async function refreshHistory() {
  const params = new URLSearchParams();
  params.set("limit", "30");
  const studyFilter = (ui.historyStudyFilter.value || "").trim();
  const caseFilter = (ui.historyCaseFilter.value || "").trim();
  if (studyFilter) params.set("study_path", studyFilter);
  if (caseFilter) params.set("case_id", caseFilter);

  const [runsPayload, casesPayload] = await Promise.all([
    callApi(`/api/history/runs?${params.toString()}`),
    callApi(`/api/history/cases?${params.toString()}&limit=120`),
  ]);
  renderHistoryRuns(runsPayload);
  renderHistoryCases(casesPayload);
  highlightSelectedHistoryRun();
}

function syncStudyPathInput() {
  const path = (currentConfig && currentConfig.study && currentConfig.study.template_model) || "";
  ui.studyPathInput.value = path;
}

function applyStudyPathIntoConfig(pathValue) {
  const path = (pathValue || "").trim();
  const parsed = JSON.parse(ui.configText.value);
  if (!parsed.study || typeof parsed.study !== "object") {
    parsed.study = {};
  }
  parsed.study.template_model = path;
  ui.configText.value = JSON.stringify(parsed, null, 2);
  currentConfig = parsed;
  updateSolveBanner();
}

function fillStudyCandidates(studies) {
  ui.studyCandidates.innerHTML = "";
  for (const item of studies) {
    const option = document.createElement("option");
    option.value = item.path;
    const dateText = item.modified_epoch
      ? new Date(item.modified_epoch * 1000).toLocaleString()
      : "-";
    option.textContent = `${item.path}  (modified: ${dateText})`;
    ui.studyCandidates.appendChild(option);
  }
}

async function loadConfig() {
  const config = await callApi("/api/config");
  currentConfig = config;
  ui.configText.value = JSON.stringify(config, null, 2);
  const llmMaxRows = config && config.llm ? config.llm.max_rows : "";
  ui.llmMaxRows.value = llmMaxRows ? String(llmMaxRows) : "";

   if (!ui.loopObjectiveAlias.value) {
    const ranking = (config && config.ranking) || [];
    if (ranking.length && ranking[0].alias) {
      ui.loopObjectiveAlias.value = String(ranking[0].alias);
      ui.loopObjectiveGoal.value = String(ranking[0].goal || "min").toLowerCase() === "max" ? "max" : "min";
    }
  }
  if (!ui.loopBatchSize.value) {
    const loopCfg = (config && config.design_loop) || {};
    if (loopCfg.batch_size_default) ui.loopBatchSize.value = String(loopCfg.batch_size_default);
    if (loopCfg.max_batches_default) ui.loopMaxBatches.value = String(loopCfg.max_batches_default);
  }
  if (!ui.loopSearchSpace.value) {
    ui.loopSearchSpace.value = JSON.stringify(
      [
        { name: "fin_height_mm", type: "real", min: 5, max: 20 },
        { name: "fin_spacing_mm", type: "real", min: 2, max: 10 },
        { name: "flow_rate_lpm", type: "real", min: 1, max: 5 },
      ],
      null,
      2
    );
  }
  if (!ui.loopConstraints.value) {
    const criteria = (config && config.criteria) || [];
    ui.loopConstraints.value = JSON.stringify(criteria, null, 2);
  }
  if (!ui.loopFixedValues.value) {
    ui.loopFixedValues.value = JSON.stringify({}, null, 2);
  }
  if (!ui.surrogateObjectiveAlias.value) {
    const ranking = (config && config.ranking) || [];
    if (ranking.length && ranking[0].alias) {
      ui.surrogateObjectiveAlias.value = String(ranking[0].alias);
      ui.surrogateObjectiveGoal.value = String(ranking[0].goal || "min").toLowerCase() === "max" ? "max" : "min";
    }
  }
  if (!ui.surrogateSampleCount.value) {
    ui.surrogateSampleCount.value = "10000";
  }
  if (!ui.surrogateTopN.value) {
    ui.surrogateTopN.value = "25";
  }
  if (!ui.surrogateValidateTopN.value) {
    ui.surrogateValidateTopN.value = "3";
  }
  if (!ui.surrogateMinRows.value) {
    ui.surrogateMinRows.value = "50";
  }
  if (!ui.surrogateSearchSpace.value) {
    ui.surrogateSearchSpace.value = JSON.stringify(
      [
        { name: "inlet_velocity_ms", type: "real", min: 1, max: 5 },
        { name: "ambient_temp_c", type: "real", min: 20, max: 40 },
        { name: "total_heat_w", type: "real", min: 50, max: 120 },
      ],
      null,
      2
    );
  }
  if (!ui.surrogateConstraints.value) {
    ui.surrogateConstraints.value = JSON.stringify((config && config.criteria) || [], null, 2);
  }
  if (!ui.surrogateFixedValues.value) {
    ui.surrogateFixedValues.value = JSON.stringify({}, null, 2);
  }
  syncStudyPathInput();
  updateSolveBanner();
}

async function saveConfig() {
  const parsed = JSON.parse(ui.configText.value);
  await callApi("/api/config", {
    method: "POST",
    body: JSON.stringify(parsed),
  });
  currentConfig = parsed;
  syncStudyPathInput();
  updateSolveBanner();
  flash("Config saved.");
}

async function loadCases() {
  const payload = await callApi("/api/cases");
  ui.casesText.value = payload.csv || "";
  renderCasesMatrix(payload.rows || []);
}

async function saveCases() {
  const payload = await callApi("/api/cases", {
    method: "POST",
    body: JSON.stringify({ csv: ui.casesText.value }),
  });
  renderCasesMatrix(payload.rows || []);
  flash("Cases CSV saved.");
}

function renderLlmResult(payload, applied) {
  const lines = [];
  lines.push(`Provider: ${payload.provider || "-"}`);
  lines.push(`Model: ${payload.model || "-"}`);
  lines.push(`Rows generated: ${payload.row_count || 0}`);
  lines.push(`Applied to cases.csv: ${applied ? "yes" : "no"}`);
  if (payload.notes) {
    lines.push("");
    lines.push("Notes:");
    lines.push(payload.notes);
  }
  ui.llmResult.textContent = lines.join("\n");
}

async function generateCasesFromPrompt(apply) {
  const prompt = (ui.llmPrompt.value || "").trim();
  if (!prompt) {
    throw new Error("Prompt is empty.");
  }
  const maxRowsText = (ui.llmMaxRows.value || "").trim();
  const body = { prompt, apply };
  if (maxRowsText) {
    const parsed = Number.parseInt(maxRowsText, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error("Max rows must be a positive integer.");
    }
    body.max_rows = parsed;
  }
  const payload = await callApi("/api/llm/generate-cases", {
    method: "POST",
    body: JSON.stringify(body),
  });
  ui.casesText.value = payload.csv || ui.casesText.value;
  renderCasesMatrix(payload.rows || []);
  renderLlmResult(payload, apply);
  flash(`LLM generated ${payload.row_count || 0} row(s).`);
}

function renderMeshSuggestion(payload, apply) {
  const lines = [];
  lines.push(`Provider: ${payload.provider || "-"}`);
  lines.push(`Model: ${payload.model || "-"}`);
  lines.push(`Applied to config: ${apply ? "yes" : "no"}`);
  lines.push("");
  lines.push("Mesh Parameters:");
  lines.push(JSON.stringify(payload.mesh_params || {}, null, 2));
  lines.push("");
  lines.push("Quality Gate:");
  lines.push(JSON.stringify(payload.quality_gate || {}, null, 2));
  if (payload.notes) {
    lines.push("");
    lines.push("Notes:");
    lines.push(payload.notes);
  }
  ui.meshResult.textContent = lines.join("\n");
}

async function suggestMeshWithLlm(apply) {
  const prompt = (ui.meshPrompt.value || "").trim();
  const payload = await callApi("/api/llm/suggest-mesh", {
    method: "POST",
    body: JSON.stringify({
      prompt,
      apply,
    }),
  });
  renderMeshSuggestion(payload, apply);
  if (apply && payload.config) {
    currentConfig = payload.config;
    ui.configText.value = JSON.stringify(payload.config, null, 2);
    updateSolveBanner();
  }
  flash(`LLM mesh suggestion generated${apply ? " and applied" : ""}.`);
}

function parseJsonField(text, fieldName, fallback) {
  const trimmed = (text || "").trim();
  if (!trimmed) return fallback;
  try {
    return JSON.parse(trimmed);
  } catch (_err) {
    throw new Error(`${fieldName} is not valid JSON.`);
  }
}

function renderCasesMatrix(rows) {
  const items = Array.isArray(rows) ? rows : [];
  ui.casesMatrixWrap.innerHTML = "";
  if (!items.length) {
    ui.casesMatrixWrap.textContent = "No case rows loaded.";
    return;
  }

  const columns = [];
  items.forEach((row) => {
    Object.keys(row || {}).forEach((key) => {
      if (!columns.includes(key)) columns.push(key);
    });
  });

  const title = document.createElement("h3");
  title.textContent = "Case Matrix";
  ui.casesMatrixWrap.appendChild(title);

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    trHead.appendChild(th);
  });
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  items.slice(0, 300).forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      const value = row[col] ?? "";
      if (col === "turbulence_model" && String(value).trim()) {
        const tag = document.createElement("span");
        tag.className = "tag turbulence";
        tag.textContent = String(value);
        td.appendChild(tag);
      } else {
        td.textContent = String(value);
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  ui.casesMatrixWrap.appendChild(table);
}

function buildDesignLoopPayload() {
  const objectiveAlias = (ui.loopObjectiveAlias.value || "").trim();
  if (!objectiveAlias) {
    throw new Error("Objective alias is required.");
  }
  const objectiveGoal = (ui.loopObjectiveGoal.value || "min").trim().toLowerCase() === "max" ? "max" : "min";
  const batchSizeText = (ui.loopBatchSize.value || "").trim();
  const maxBatchesText = (ui.loopMaxBatches.value || "").trim();
  const batchSize = batchSizeText ? Number.parseInt(batchSizeText, 10) : undefined;
  const maxBatches = maxBatchesText ? Number.parseInt(maxBatchesText, 10) : undefined;
  if (batchSize !== undefined && (!Number.isFinite(batchSize) || batchSize <= 0)) {
    throw new Error("Batch size must be a positive integer.");
  }
  if (maxBatches !== undefined && (!Number.isFinite(maxBatches) || maxBatches <= 0)) {
    throw new Error("Max batches must be a positive integer.");
  }

  const searchSpace = parseJsonField(ui.loopSearchSpace.value, "Search Space", []);
  const constraints = parseJsonField(ui.loopConstraints.value, "Constraints", []);
  const fixedValues = parseJsonField(ui.loopFixedValues.value, "Fixed Values", {});

  if (!Array.isArray(searchSpace) || !searchSpace.length) {
    throw new Error("Search Space must be a non-empty JSON array.");
  }
  if (!Array.isArray(constraints)) {
    throw new Error("Constraints must be a JSON array.");
  }
  if (typeof fixedValues !== "object" || Array.isArray(fixedValues) || fixedValues === null) {
    throw new Error("Fixed Values must be a JSON object.");
  }

  return {
    objective_alias: objectiveAlias,
    objective_goal: objectiveGoal,
    batch_size: batchSize,
    max_batches: maxBatches,
    search_space: searchSpace,
    constraints,
    fixed_values: fixedValues,
  };
}

function getLoopBatches(payload) {
  const summary = (payload && payload.last_summary) || {};
  const history = Array.isArray(summary.history) ? summary.history : [];
  if (history.length) return history;
  const timeline = Array.isArray(payload && payload.batch_timeline) ? payload.batch_timeline : [];
  return timeline;
}

function getBatchBestCase(batch) {
  if (batch && batch.best_case_in_batch) return batch.best_case_in_batch;
  if (batch && batch.best_case) return batch.best_case;
  return {};
}

function getBatchCases(batch) {
  return Array.isArray(batch && batch.cases) ? batch.cases : [];
}

function renderLoopPreflight(payload) {
  const summary = (payload && payload.last_summary) || {};
  const preflight =
    (payload && payload.preflight) ||
    (summary && summary.metric_contract_preflight) ||
    {};
  const line = [];
  if (preflight && preflight.skipped) {
    line.push(`Preflight: skipped (${preflight.reason || "unknown"})`);
  } else if (preflight && preflight.ok) {
    line.push("Preflight: passed");
    if (preflight.checked_metrics !== undefined) line.push(`checked=${preflight.checked_metrics}`);
    if (preflight.available_metric_pairs !== undefined) line.push(`available=${preflight.available_metric_pairs}`);
  } else if (preflight && Object.keys(preflight).length) {
    line.push("Preflight: failed");
  } else {
    line.push("Preflight: waiting");
  }
  ui.loopPreflight.textContent = line.join(" | ");
}

function renderLoopTimeline(payload, batches) {
  ui.loopTimelineWrap.innerHTML = "";
  if (!batches.length) return;
  const total = batches.length;
  const wrap = document.createElement("div");
  wrap.className = "timeline";
  batches.forEach((batch, idx) => {
    const batchIndex = batch.batch_index || idx + 1;
    const best = getBatchBestCase(batch);
    const stage = idx < Math.ceil(total * 0.5) ? "exploration" : "exploitation";
    const caseCount = getBatchCases(batch).length || batch.case_count || 0;
    let feasibleCount = batch.feasible_count;
    if (feasibleCount === undefined) {
      feasibleCount = getBatchCases(batch).filter((item) => item && item.constraints_pass).length;
    }
    const chip = document.createElement("div");
    chip.className = `timeline-item ${stage}`;
    chip.textContent =
      `B${batchIndex} ${stage} | best=${best.case_id || "-"} ` +
      `obj=${best.objective_value ?? "-"} | feasible=${feasibleCount}/${caseCount || "-"}`;
    wrap.appendChild(chip);
  });
  ui.loopTimelineWrap.appendChild(wrap);
}

function renderLoopBatchTable(batches) {
  ui.loopBatchTableWrap.innerHTML = "";
  const rows = [];
  batches.forEach((batch, idx) => {
    const batchIndex = batch.batch_index || idx + 1;
    getBatchCases(batch).forEach((item) => {
      rows.push({
        batch_index: batchIndex,
        case_id: item.case_id || "-",
        success: Boolean(item.success),
        objective_value: item.objective_value,
        score: item.score,
        failure_type: item.failure_type || "",
      });
    });
  });
  if (!rows.length) return;

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  ["batch", "case", "status", "objective", "score", "failure_type"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    trHead.appendChild(th);
  });
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.slice(-120).forEach((row) => {
    const tr = document.createElement("tr");
    const status = row.success ? "success" : "failed";
    const values = [row.batch_index, row.case_id, status, row.objective_value ?? "-", row.score ?? "-", row.failure_type || ""];
    values.forEach((value, colIdx) => {
      const td = document.createElement("td");
      if (colIdx === 2) {
        const tag = document.createElement("span");
        tag.className = `tag ${status}`;
        tag.textContent = status;
        td.appendChild(tag);
      } else if (colIdx === 5 && value) {
        const failTag = document.createElement("span");
        failTag.className = `failure-tag failure-${normalizeFailureType(value)}`;
        failTag.textContent = String(value);
        td.appendChild(failTag);
      } else {
        td.textContent = String(value);
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  ui.loopBatchTableWrap.appendChild(table);
}

function renderLoopConvergence(payload, batches) {
  const canvas = ui.loopConvergenceChart;
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const rect = canvas.getBoundingClientRect();
  const width = Math.max(320, Math.floor(rect.width || 640));
  const height = 180;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const summary = payload.last_summary || {};
  const goal = String(summary.objective_goal || "min").toLowerCase() === "max" ? "max" : "min";
  const points = batches.map((batch, idx) => {
    const best = getBatchBestCase(batch);
    return {
      x: idx + 1,
      y: toNumber(best.objective_value),
    };
  }).filter((p) => p.y !== null);

  if (!points.length) {
    ctx.fillStyle = "#334155";
    ctx.font = "12px Consolas, monospace";
    ctx.fillText("Convergence chart waits for batch objective values.", 12, 26);
    return;
  }

  const margin = { left: 42, right: 12, top: 10, bottom: 28 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const xs = points.map((p) => p.x);
  const ys = points.map((p) => Number(p.y));
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const yRange = maxY - minY || 1;

  const xToPx = (x) => margin.left + ((x - minX) / Math.max(1, maxX - minX)) * innerW;
  const yToPx = (y) => margin.top + ((maxY - y) / yRange) * innerH;

  ctx.strokeStyle = "#cbd5e1";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(margin.left, margin.top);
  ctx.lineTo(margin.left, margin.top + innerH);
  ctx.lineTo(margin.left + innerW, margin.top + innerH);
  ctx.stroke();

  for (let i = 1; i <= batches.length; i += 1) {
    const x = xToPx(i);
    ctx.strokeStyle = "rgba(100,116,139,0.24)";
    ctx.beginPath();
    ctx.moveTo(x, margin.top);
    ctx.lineTo(x, margin.top + innerH);
    ctx.stroke();
  }

  let runningBest = goal === "max" ? -Number.MAX_VALUE : Number.MAX_VALUE;
  const bestSeries = points.map((p) => {
    runningBest = goal === "max" ? Math.max(runningBest, Number(p.y)) : Math.min(runningBest, Number(p.y));
    return { x: p.x, y: runningBest };
  });

  ctx.strokeStyle = "#0ea5a3";
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((p, idx) => {
    const px = xToPx(p.x);
    const py = yToPx(Number(p.y));
    if (idx === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  });
  ctx.stroke();

  ctx.strokeStyle = "#0f172a";
  ctx.setLineDash([5, 3]);
  ctx.beginPath();
  bestSeries.forEach((p, idx) => {
    const px = xToPx(p.x);
    const py = yToPx(Number(p.y));
    if (idx === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  });
  ctx.stroke();
  ctx.setLineDash([]);

  ctx.fillStyle = "#0f172a";
  ctx.font = "11px Consolas, monospace";
  ctx.fillText(`Goal: ${goal}`, margin.left, height - 8);
  ctx.fillText(`best=${runningBest.toFixed(4)}`, Math.max(margin.left, width - 150), height - 8);
}

function renderDesignLoopStatus(payload) {
  latestDesignLoopStatus = payload || {};
  const lines = [];
  const summary = payload.last_summary || {};
  const optimizerMode = payload.optimizer_mode || summary.optimizer_mode || "-";
  const optimizerWarning = payload.optimizer_warning || summary.optimizer_warning || "";
  lines.push(`Running: ${payload.running ? "yes" : "no"}`);
  lines.push(`Status: ${payload.status || "-"}`);
  lines.push(`Loop ID: ${payload.loop_id || "-"}`);
  lines.push(`Batch: ${payload.current_batch || 0} / ${payload.max_batches || 0}`);
  lines.push(`Completed Batches: ${payload.completed_batches || 0}`);
  lines.push(`Optimizer: ${optimizerMode}`);
  if (optimizerWarning) {
    lines.push(`WARNING: ${optimizerWarning}`);
  }
  if (payload.last_error) {
    lines.push(`Last Error: ${payload.last_error}`);
  }

  if (summary.best_case) {
    lines.push("");
    lines.push("Best Case:");
    lines.push(JSON.stringify(summary.best_case, null, 2));
  }

  const logs = (payload.logs || []).slice(-12);
  if (logs.length) {
    lines.push("");
    lines.push("Recent Logs:");
    for (const line of logs) lines.push(line);
  }

  ui.loopStatus.textContent = lines.join("\n");
  const batches = getLoopBatches(payload);
  renderLoopPreflight(payload);
  renderLoopTimeline(payload, batches);
  renderLoopBatchTable(batches);
  renderLoopConvergence(payload, batches);
  if (latestRunSummary && Object.keys(latestRunSummary).length) {
    renderSummary(latestRunSummary);
  }
}

async function startDesignLoop() {
  const body = buildDesignLoopPayload();
  const payload = await callApi("/api/design-loop/start", {
    method: "POST",
    body: JSON.stringify(body),
  });
  flash(payload.message || "Design loop started.");
}

async function stopDesignLoop() {
  const payload = await callApi("/api/design-loop/stop", {
    method: "POST",
    body: JSON.stringify({}),
  });
  flash(payload.message || "Design loop stop requested.");
}

async function refreshDesignLoopStatus() {
  const [statusPayload, latestPayload] = await Promise.all([
    callApi("/api/design-loop/status"),
    callApi("/api/design-loop/latest"),
  ]);
  const merged = { ...(statusPayload || {}) };
  const hasSummary = merged.last_summary && Object.keys(merged.last_summary).length > 0;
  if (!hasSummary && latestPayload && Object.keys(latestPayload).length > 0) {
    merged.last_summary = latestPayload;
  }
  renderDesignLoopStatus(merged);
}

function renderCoverageMap(coveragePayload) {
  if (!coveragePayload || !coveragePayload.map || !Array.isArray(coveragePayload.map.cells)) {
    ui.surrogateCoverage.textContent = "Coverage map unavailable.";
    return;
  }
  const map = coveragePayload.map;
  const cells = map.cells || [];
  if (!cells.length) {
    ui.surrogateCoverage.textContent = "Coverage map unavailable.";
    return;
  }
  const symbols = { 0: "░", 1: "▓", 2: "█" };
  const lines = [];
  lines.push(`Coverage Map: ${map.y_feature || "-"} vs ${map.x_feature || "-"}`);
  for (const row of cells) {
    const chars = row.map((value) => symbols[value] || "░").join("");
    lines.push(chars);
  }
  lines.push("Legend: █ high  ▓ medium  ░ low");
  ui.surrogateCoverage.textContent = lines.join("\n");
}

function renderSurrogateFeatureBars(coveragePayload) {
  ui.surrogateFeatureBars.innerHTML = "";
  const perFeature = coveragePayload && coveragePayload.per_feature ? coveragePayload.per_feature : {};
  const entries = Object.entries(perFeature || {});
  if (!entries.length) {
    ui.surrogateFeatureBars.textContent = "Feature coverage bars unavailable.";
    return;
  }

  const wrap = document.createElement("div");
  wrap.className = "feature-bars";
  entries
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .forEach(([name, value]) => {
      const row = document.createElement("div");
      row.className = "feature-row";

      const label = document.createElement("div");
      label.className = "feature-label";
      label.textContent = `${name}`;
      row.appendChild(label);

      const bar = document.createElement("div");
      bar.className = "feature-bar";
      const fill = document.createElement("div");
      fill.className = "feature-fill";
      fill.style.width = `${Math.max(0, Math.min(100, Number(value) * 100)).toFixed(1)}%`;
      bar.appendChild(fill);
      row.appendChild(bar);

      const pct = document.createElement("div");
      pct.className = "feature-pct";
      pct.textContent = `${(Number(value) * 100).toFixed(1)}%`;
      row.appendChild(pct);

      wrap.appendChild(row);
    });
  ui.surrogateFeatureBars.appendChild(wrap);
}

function renderSurrogateWorkflow(statusResult) {
  const trained = Boolean(statusResult && statusResult.trained);
  const ready = Boolean(statusResult && statusResult.ready);
  const predicted = Boolean(surrogatePredictState.predictedAt);
  const validating = Boolean(surrogatePredictState.validating) || (latestRunStatus.running && latestRunStatus.mode === "validate");

  const steps = [
    { label: "1) Train", done: trained, active: !trained },
    { label: "2) Predict", done: predicted, active: trained && !predicted },
    { label: "3) Validate", done: ready && !validating, active: validating || (predicted && !ready) },
  ];
  ui.surrogateWorkflow.innerHTML = "";
  const wrap = document.createElement("div");
  wrap.className = "workflow-steps";
  steps.forEach((step) => {
    const item = document.createElement("span");
    item.className = `workflow-step ${step.done ? "done" : step.active ? "active" : "pending"}`;
    item.textContent = step.label;
    wrap.appendChild(item);
  });
  ui.surrogateWorkflow.appendChild(wrap);
}

function renderSurrogateStatus(statusPayload, coveragePayload) {
  const result = (statusPayload && statusPayload.result) || {};
  const coverage = (coveragePayload && coveragePayload.result) || {};
  const lines = [];
  lines.push(`Trained: ${result.trained ? "yes" : "no"}`);
  lines.push(`Ready: ${result.ready ? "yes" : "no"}`);
  lines.push(`Model: ${result.model_name || "-"}`);
  lines.push(`Objective alias: ${result.target_alias || "-"}`);
  lines.push(`Rows: ${result.row_count || 0}`);
  const r2 = Number(result.best_r2);
  if (Number.isFinite(r2)) {
    lines.push(`R2: ${r2.toFixed(4)}`);
  }
  const coverageOverall = Number(result.coverage && result.coverage.overall);
  if (Number.isFinite(coverageOverall)) {
    lines.push(`Coverage: ${(coverageOverall * 100).toFixed(1)}%`);
  }
  if (result.message) {
    lines.push(`Info: ${result.message}`);
  }
  if (result.training_data_csv) {
    lines.push(`Training data: ${result.training_data_csv}`);
  }
  ui.surrogateStatus.textContent = lines.join("\n");
  renderCoverageMap(coverage);
  renderSurrogateFeatureBars(coverage);
  renderSurrogateWorkflow(result);
}

function buildSurrogatePayload() {
  const objectiveAlias = (ui.surrogateObjectiveAlias.value || "").trim();
  if (!objectiveAlias) {
    throw new Error("Surrogate objective alias is required.");
  }
  const objectiveGoal = (ui.surrogateObjectiveGoal.value || "min").trim().toLowerCase() === "max" ? "max" : "min";
  const sampleCount = Number.parseInt((ui.surrogateSampleCount.value || "10000").trim(), 10);
  const topN = Number.parseInt((ui.surrogateTopN.value || "25").trim(), 10);
  const validateTopN = Number.parseInt((ui.surrogateValidateTopN.value || "3").trim(), 10);
  const minRows = Number.parseInt((ui.surrogateMinRows.value || "50").trim(), 10);
  if (!Number.isFinite(sampleCount) || sampleCount <= 0) {
    throw new Error("Sample count must be a positive integer.");
  }
  if (!Number.isFinite(topN) || topN <= 0) {
    throw new Error("Top N must be a positive integer.");
  }
  if (!Number.isFinite(validateTopN) || validateTopN <= 0) {
    throw new Error("Validate Top N must be a positive integer.");
  }
  if (!Number.isFinite(minRows) || minRows <= 0) {
    throw new Error("Train min rows must be a positive integer.");
  }

  const searchSpace = parseJsonField(ui.surrogateSearchSpace.value, "Surrogate Search Space", []);
  const constraints = parseJsonField(ui.surrogateConstraints.value, "Surrogate Constraints", []);
  const fixedValues = parseJsonField(ui.surrogateFixedValues.value, "Surrogate Fixed Values", {});
  if (!Array.isArray(searchSpace) || !searchSpace.length) {
    throw new Error("Surrogate Search Space must be a non-empty JSON array.");
  }
  if (!Array.isArray(constraints)) {
    throw new Error("Surrogate Constraints must be a JSON array.");
  }
  if (typeof fixedValues !== "object" || fixedValues === null || Array.isArray(fixedValues)) {
    throw new Error("Surrogate Fixed Values must be a JSON object.");
  }

  return {
    objective_alias: objectiveAlias,
    objective_goal: objectiveGoal,
    sample_count: sampleCount,
    top_n: topN,
    validate_top_n: validateTopN,
    min_rows: minRows,
    search_space: searchSpace,
    constraints,
    fixed_values: fixedValues,
  };
}

async function refreshSurrogateStatus() {
  const [statusPayload, coveragePayload] = await Promise.all([
    callApi("/api/surrogate/status"),
    callApi("/api/surrogate/coverage"),
  ]);
  renderSurrogateStatus(statusPayload, coveragePayload);
}

async function trainSurrogate() {
  const body = buildSurrogatePayload();
  const payload = await callApi("/api/surrogate/train", {
    method: "POST",
    body: JSON.stringify({
      objective_alias: body.objective_alias,
      min_rows: body.min_rows,
      include_design_loops: true,
    }),
  });
  flash(`Surrogate training complete: ${payload.result.model_name || "model selected"}.`);
  await refreshSurrogateStatus();
}

async function predictSurrogate() {
  const body = buildSurrogatePayload();
  const payload = await callApi("/api/surrogate/predict", {
    method: "POST",
    body: JSON.stringify(body),
  });
  const result = payload.result || {};
  const lines = [];
  lines.push(`Rows evaluated: ${result.rows_evaluated || result.sample_count || 0}`);
  lines.push(`Model: ${result.model_name || "-"}`);
  if (Number.isFinite(result.best_r2)) {
    lines.push(`R2: ${Number(result.best_r2).toFixed(4)}`);
  }
  lines.push(`Low-confidence cases: ${result.low_confidence_count || 0}`);
  lines.push("");
  lines.push("Top candidates:");
  const top = Array.isArray(result.top_candidates) ? result.top_candidates : [];
  if (!top.length) {
    lines.push("(none)");
  } else {
    for (const item of top.slice(0, 20)) {
      lines.push(
        `#${item.rank || "-"} ${item.case_id || "-"} ` +
        `pred=${item.prediction} conf=${(Number(item.confidence || 0) * 100).toFixed(1)}% ` +
        `(${item.confidence_level || "low"})`
      );
      lines.push(`  params=${JSON.stringify(item.params || {})}`);
      if (Array.isArray(item.constraint_violations) && item.constraint_violations.length) {
        lines.push(`  violations=${item.constraint_violations.join("; ")}`);
      }
    }
  }
  if (Array.isArray(result.warnings) && result.warnings.length) {
    lines.push("");
    lines.push("Warnings:");
    result.warnings.slice(0, 10).forEach((line) => lines.push(`- ${line}`));
  }
  ui.surrogatePredictOutput.textContent = lines.join("\n");
  surrogatePredictState.predictedAt = new Date().toISOString();
  flash(`Surrogate predicted ${result.rows_evaluated || 0} combinations.`);
}

async function validateSurrogate() {
  const body = buildSurrogatePayload();
  const payload = await callApi("/api/run", {
    method: "POST",
    body: JSON.stringify({
      mode: "validate",
      objective_alias: body.objective_alias,
      objective_goal: body.objective_goal,
      sample_count: body.sample_count,
      top_n: body.top_n,
      validate_top_n: body.validate_top_n,
      search_space: body.search_space,
      constraints: body.constraints,
      fixed_values: body.fixed_values,
      auto_retrain: true,
      retrain_min_rows: body.min_rows,
    }),
  });
  surrogatePredictState.validating = true;
  flash(payload.message || "Validate mode started.");
}

async function discoverStudies() {
  const payload = await callApi("/api/studies");
  fillStudyCandidates(payload.studies || []);
  flash(`Discovered ${payload.count || 0} study file(s).`);
}

async function runIntrospection() {
  ui.introspection.textContent = "Running introspection...";
  const payload = await callApi("/api/introspect", {
    method: "POST",
    body: JSON.stringify({ study_path: ui.studyPathInput.value.trim() || undefined }),
  });
  ui.introspection.textContent = JSON.stringify(payload.result.data || {}, null, 2);
  flash("Introspection completed.");
}

async function startRun(mode) {
  const payload = await callApi("/api/run", {
    method: "POST",
    body: JSON.stringify({ mode }),
  });
  flash(payload.message || `Run ${mode} triggered.`);
}

async function refreshStatus() {
  const status = await callApi("/api/status");
  updateStatusView(status);
}

async function refreshLatestRun() {
  const summary = await callApi("/api/latest-run");
  latestRunSummary = summary || {};
  renderSummary(latestRunSummary);
}

async function boot() {
  try {
    await Promise.all([
      loadConfig(),
      loadCases(),
      refreshStatus(),
      refreshLatestRun(),
      refreshHistory(),
      refreshDesignLoopStatus(),
      refreshSurrogateStatus(),
    ]);
  } catch (err) {
    flash(`Initial load failed: ${err.message}`);
  }
}

ui.apiKeyInput.value = localStorage.getItem("cfd_api_key") || "";
ui.apiKeyInput.addEventListener("change", persistApiKey);
ui.apiKeyInput.addEventListener("keyup", (event) => {
  if (event.key === "Enter") {
    persistApiKey();
    flash("API key updated.");
  }
});

ui.reloadBtn.addEventListener("click", async () => {
  await boot();
  flash("Reloaded config, cases, status, and latest run.");
});

ui.historyRefreshBtn.addEventListener("click", async () => {
  try {
    await refreshHistory();
    flash("History refreshed.");
  } catch (err) {
    flash(`History refresh failed: ${err.message}`);
  }
});

ui.historyStudyFilter.addEventListener("keyup", async (event) => {
  if (event.key !== "Enter") return;
  try {
    await refreshHistory();
  } catch (err) {
    flash(`History filter failed: ${err.message}`);
  }
});

ui.historyCaseFilter.addEventListener("keyup", async (event) => {
  if (event.key !== "Enter") return;
  try {
    await refreshHistory();
  } catch (err) {
    flash(`History filter failed: ${err.message}`);
  }
});

ui.saveConfigBtn.addEventListener("click", async () => {
  try {
    await saveConfig();
  } catch (err) {
    flash(`Save config failed: ${err.message}`);
  }
});

ui.saveCasesBtn.addEventListener("click", async () => {
  try {
    await saveCases();
  } catch (err) {
    flash(`Save cases failed: ${err.message}`);
  }
});

ui.discoverStudiesBtn.addEventListener("click", async () => {
  try {
    await discoverStudies();
  } catch (err) {
    flash(`Study discovery failed: ${err.message}`);
  }
});

ui.applyStudyPathBtn.addEventListener("click", () => {
  try {
    applyStudyPathIntoConfig(ui.studyPathInput.value);
    flash("Study path applied to config editor. Click 'Save Config' to persist.");
  } catch (err) {
    flash(`Apply study path failed: ${err.message}`);
  }
});

ui.useSelectedStudyBtn.addEventListener("click", () => {
  const selected = ui.studyCandidates.value;
  if (!selected) {
    flash("No discovered study selected.");
    return;
  }
  ui.studyPathInput.value = selected;
  try {
    applyStudyPathIntoConfig(selected);
    flash("Selected discovered study path applied to config editor.");
  } catch (err) {
    flash(`Use selected study failed: ${err.message}`);
  }
});

ui.introspectBtn.addEventListener("click", async () => {
  try {
    await runIntrospection();
  } catch (err) {
    ui.introspection.textContent = `Introspection failed: ${err.message}`;
    flash(`Introspection failed: ${err.message}`);
  }
});

ui.runAllBtn.addEventListener("click", async () => {
  try {
    await startRun("all");
  } catch (err) {
    flash(`Run all failed: ${err.message}`);
  }
});

ui.runFailedBtn.addEventListener("click", async () => {
  try {
    await startRun("failed");
  } catch (err) {
    flash(`Rerun failed failed: ${err.message}`);
  }
});

ui.runChangedBtn.addEventListener("click", async () => {
  try {
    await startRun("changed");
  } catch (err) {
    flash(`Rerun changed failed: ${err.message}`);
  }
});

ui.llmPreviewBtn.addEventListener("click", async () => {
  try {
    await generateCasesFromPrompt(false);
  } catch (err) {
    flash(`LLM preview failed: ${err.message}`);
    ui.llmResult.textContent = `LLM preview failed: ${err.message}`;
  }
});

ui.llmApplyBtn.addEventListener("click", async () => {
  try {
    await generateCasesFromPrompt(true);
    flash("Generated cases applied to cases.csv.");
  } catch (err) {
    flash(`LLM apply failed: ${err.message}`);
    ui.llmResult.textContent = `LLM apply failed: ${err.message}`;
  }
});

ui.meshSuggestBtn.addEventListener("click", async () => {
  try {
    await suggestMeshWithLlm(false);
  } catch (err) {
    flash(`Mesh suggestion failed: ${err.message}`);
    ui.meshResult.textContent = `Mesh suggestion failed: ${err.message}`;
  }
});

ui.meshApplyBtn.addEventListener("click", async () => {
  try {
    await suggestMeshWithLlm(true);
  } catch (err) {
    flash(`Mesh apply failed: ${err.message}`);
    ui.meshResult.textContent = `Mesh apply failed: ${err.message}`;
  }
});

ui.loopStartBtn.addEventListener("click", async () => {
  try {
    await startDesignLoop();
    await refreshDesignLoopStatus();
  } catch (err) {
    flash(`Start design loop failed: ${err.message}`);
    ui.loopStatus.textContent = `Start design loop failed: ${err.message}`;
  }
});

ui.loopStopBtn.addEventListener("click", async () => {
  try {
    await stopDesignLoop();
    await refreshDesignLoopStatus();
  } catch (err) {
    flash(`Stop design loop failed: ${err.message}`);
    ui.loopStatus.textContent = `Stop design loop failed: ${err.message}`;
  }
});

ui.surrogateTrainBtn.addEventListener("click", async () => {
  try {
    await trainSurrogate();
  } catch (err) {
    flash(`Surrogate train failed: ${err.message}`);
    ui.surrogateStatus.textContent = `Surrogate train failed: ${err.message}`;
  }
});

ui.surrogateRefreshBtn.addEventListener("click", async () => {
  try {
    await refreshSurrogateStatus();
    flash("Surrogate status refreshed.");
  } catch (err) {
    flash(`Surrogate refresh failed: ${err.message}`);
  }
});

ui.surrogatePredictBtn.addEventListener("click", async () => {
  try {
    await predictSurrogate();
  } catch (err) {
    flash(`Surrogate predict failed: ${err.message}`);
    ui.surrogatePredictOutput.textContent = `Surrogate predict failed: ${err.message}`;
  }
});

ui.surrogateValidateBtn.addEventListener("click", async () => {
  try {
    await validateSurrogate();
  } catch (err) {
    flash(`Surrogate validate failed: ${err.message}`);
    ui.surrogatePredictOutput.textContent = `Surrogate validate failed: ${err.message}`;
  }
});

setInterval(async () => {
  try {
    await refreshStatus();
    await refreshLatestRun();
    await refreshDesignLoopStatus();
    await refreshSurrogateStatus();
  } catch (err) {
    flash(`Auto-refresh failed: ${err.message}`);
  }
}, 1000);

boot();
