(function () {
  "use strict";

  const ui = {
    navRunningBadge: document.getElementById("navRunningBadge"),
    activeStudyName: document.getElementById("activeStudyName"),
    activeStudyStatus: document.getElementById("activeStudyStatus"),
    monitorSubtitle: document.getElementById("monitorSubtitle"),
    logContainer: document.getElementById("logContainer"),
    historyRefreshBtn: document.getElementById("historyRefreshBtn"),
    historyQuickChips: document.getElementById("historyQuickChips"),
    historyRunMeta: document.getElementById("historyRunMeta"),
    historyRunsBody: document.getElementById("historyRunsBody"),
    historySummaryMeta: document.getElementById("historySummaryMeta"),
    historySummaryGrid: document.getElementById("historySummaryGrid"),
    historyCasesBody: document.getElementById("historyCasesBody"),
  };

  const state = {
    selectedHistoryRunId: "",
    historyRuns: [],
  };

  function getApiKey() {
    return (localStorage.getItem("cfd_api_key") || "").trim();
  }

  async function callApi(url) {
    const headers = {};
    const apiKey = getApiKey();
    if (apiKey) headers["X-API-Key"] = apiKey;
    const response = await fetch(url, { headers });
    let payload = {};
    try {
      payload = await response.json();
    } catch (_err) {
      payload = {};
    }
    if (!response.ok) {
      const err = payload.error || `Request failed: ${url}`;
      throw new Error(err);
    }
    return payload;
  }

  function basename(pathValue) {
    const text = String(pathValue || "").trim();
    if (!text) return "-";
    const parts = text.replace(/\\/g, "/").split("/");
    return parts[parts.length - 1] || text;
  }

  function formatDate(value) {
    const text = String(value || "").trim();
    if (!text) return "-";
    const dt = new Date(text);
    if (Number.isNaN(dt.getTime())) return text;
    return dt.toLocaleString();
  }

  function classifyLog(line) {
    const text = String(line || "").toLowerCase();
    if (!text) return "dim";
    if (/(error|failed|python_exception|non_zero_exit)/.test(text)) return "error";
    if (/(warn|warning|retry|null_metric|bad_mesh|timeout)/.test(text)) return "warn";
    if (/(pass|success|succeeded|converged|finished)/.test(text)) return "success";
    if (/(info|started|study|batch|running)/.test(text)) return "info";
    return "dim";
  }

  function splitStampedLog(line) {
    const text = String(line || "");
    const match = text.match(/^\[(.*?)\]\s*(.*)$/);
    if (!match) return { time: "", message: text };
    const maybeDate = new Date(match[1]);
    if (Number.isNaN(maybeDate.getTime())) {
      return { time: "", message: text };
    }
    const hh = String(maybeDate.getHours()).padStart(2, "0");
    const mm = String(maybeDate.getMinutes()).padStart(2, "0");
    const ss = String(maybeDate.getSeconds()).padStart(2, "0");
    return { time: `${hh}:${mm}:${ss}`, message: match[2] || "" };
  }

  function renderLogs(lines) {
    if (!ui.logContainer) return;
    const data = Array.isArray(lines) ? lines.slice(-180) : [];
    const pinnedToBottom =
      ui.logContainer.scrollTop + ui.logContainer.clientHeight >= ui.logContainer.scrollHeight - 20;
    ui.logContainer.innerHTML = "";
    data.forEach((raw) => {
      const parts = splitStampedLog(raw);
      const row = document.createElement("div");
      row.className = "log-line";

      const time = document.createElement("span");
      time.className = "log-time";
      time.textContent = parts.time || "--:--:--";
      row.appendChild(time);

      const msg = document.createElement("span");
      msg.className = `log-msg ${classifyLog(parts.message)}`;
      msg.textContent = parts.message || String(raw || "");
      row.appendChild(msg);
      ui.logContainer.appendChild(row);
    });
    if (pinnedToBottom) {
      ui.logContainer.scrollTop = ui.logContainer.scrollHeight;
    }
  }

  function summarizeMode(status) {
    if (!status || !status.running) {
      const last = status && status.last_summary ? status.last_summary : {};
      if (last.run_id) return `Idle · last run ${last.run_id}`;
      return "Idle";
    }
    const mode = String(status.mode || "all");
    const done = Number(status.completed_case_count || 0);
    const total = Number(status.selected_case_count || 0);
    const current = status.current_case ? `${status.current_case}` : "-";
    const phase = status.current_phase ? `${status.current_phase}` : "startup";
    return `Running ${mode} · ${done}/${total} · ${current} (${phase})`;
  }

  function countRunningCases(status) {
    const rows = Array.isArray(status.case_table) ? status.case_table : [];
    const running = rows.filter((row) => {
      const st = String(row.status || "").toLowerCase();
      return st === "running" || st === "retrying";
    }).length;
    if (running > 0) return running;
    return status.running ? 1 : 0;
  }

  async function refreshStatus() {
    const [status, cfg] = await Promise.all([callApi("/api/status"), callApi("/api/config")]);
    if (ui.monitorSubtitle) ui.monitorSubtitle.textContent = summarizeMode(status);
    if (ui.navRunningBadge) ui.navRunningBadge.textContent = String(countRunningCases(status));
    if (ui.activeStudyName) {
      const studyPath =
        cfg &&
        cfg.study &&
        typeof cfg.study === "object" &&
        typeof cfg.study.template_model === "string"
          ? cfg.study.template_model
          : "";
      ui.activeStudyName.textContent = basename(studyPath);
    }
    if (ui.activeStudyStatus) ui.activeStudyStatus.textContent = status.running ? "Run active" : "Idle";
    renderLogs(status.logs || []);
  }

  function renderHistorySummary(summary) {
    if (!ui.historySummaryMeta || !ui.historySummaryGrid) return;
    if (!summary || typeof summary !== "object" || !summary.run_id) {
      ui.historySummaryMeta.textContent = "No run selected";
      ui.historySummaryGrid.innerHTML = "";
      return;
    }
    ui.historySummaryMeta.textContent = `${summary.run_id} · ${formatDate(summary.created_at)}`;
    const items = [
      ["run_id", summary.run_id],
      ["date", formatDate(summary.created_at)],
      ["mode", summary.mode || "-"],
      ["study", summary.study_path || "-"],
      ["design", summary.design_name || "-"],
      ["scenario", summary.scenario_name || "-"],
      ["selected", summary.selected_case_count ?? "-"],
      ["success", summary.successful_cases ?? "-"],
      ["failed", summary.failed_cases ?? "-"],
    ];
    ui.historySummaryGrid.innerHTML = "";
    items.forEach(([key, value]) => {
      const box = document.createElement("div");
      box.className = "summary-item";
      box.innerHTML = `<div class="summary-key">${key}</div><div class="summary-val">${String(value ?? "-")}</div>`;
      ui.historySummaryGrid.appendChild(box);
    });
  }

  function renderHistoryCases(summary) {
    if (!ui.historyCasesBody) return;
    ui.historyCasesBody.innerHTML = "";
    const rows = Array.isArray(summary && summary.case_results) ? summary.case_results : [];
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="4" style="color:var(--text-muted)">No case rows in this run.</td>`;
      ui.historyCasesBody.appendChild(tr);
      return;
    }
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const ok = Boolean(row.success);
      const statusBadge = ok
        ? `<span class="badge badge-pass">Pass</span>`
        : `<span class="badge badge-fail">Fail</span>`;
      const failure = ok
        ? "-"
        : `${String(row.failure_type || "unknown")}${row.failure_reason ? ` · ${row.failure_reason}` : ""}`;

      const metrics = row.metrics && typeof row.metrics === "object" ? row.metrics : {};
      const preview = Object.keys(metrics)
        .slice(0, 3)
        .map((key) => `${key}=${metrics[key]}`)
        .join(", ");
      tr.innerHTML = `
        <td class="mono">${String(row.case_id || "-")}</td>
        <td>${statusBadge}</td>
        <td>${failure || "-"}</td>
        <td class="mono" style="font-size:11px;color:var(--text-secondary)">${preview || "-"}</td>
      `;
      ui.historyCasesBody.appendChild(tr);
    });
  }

  async function selectHistoryRun(runId) {
    const id = String(runId || "").trim();
    if (!id) return;
    state.selectedHistoryRunId = id;
    const summaryPayload = await callApi(`/api/history/runs/${encodeURIComponent(id)}`);
    const summary = summaryPayload && summaryPayload.summary ? summaryPayload.summary : {};
    renderHistorySummary(summary);
    renderHistoryCases(summary);
    renderHistoryRunsTable();
  }

  function renderQuickChips() {
    if (!ui.historyQuickChips) return;
    ui.historyQuickChips.innerHTML = "";
    const runs = state.historyRuns;
    if (!runs.length) return;

    const makeChip = (label, runId) => {
      const chip = document.createElement("button");
      chip.className = "quick-chip";
      chip.textContent = label;
      chip.addEventListener("click", () => {
        selectHistoryRun(runId).catch(() => {});
      });
      return chip;
    };

    const latest = runs[0];
    ui.historyQuickChips.appendChild(
      makeChip(`Latest · ${formatDate(latest.created_at)}`, latest.run_id)
    );

    const kani = runs.find((run) => String(run.study_path || "").toLowerCase().includes("kani yawa"));
    if (kani) {
      ui.historyQuickChips.appendChild(
        makeChip(`Kani Yawa · ${formatDate(kani.created_at)}`, kani.run_id)
      );
    }
  }

  function renderHistoryRunsTable() {
    if (!ui.historyRunsBody) return;
    ui.historyRunsBody.innerHTML = "";
    const runs = state.historyRuns;
    runs.forEach((run) => {
      const tr = document.createElement("tr");
      tr.className = "clickable";
      if (state.selectedHistoryRunId === run.run_id) tr.classList.add("selected");
      tr.addEventListener("click", () => {
        selectHistoryRun(run.run_id).catch(() => {});
      });
      const studyText = basename(run.study_path || "-");
      tr.innerHTML = `
        <td class="mono">${formatDate(run.created_at)}</td>
        <td class="mono">${String(run.run_id || "-")}</td>
        <td title="${String(run.study_path || "")}">${studyText}</td>
        <td class="mono">${String(run.successful_cases || 0)}/${String(run.failed_cases || 0)}</td>
      `;
      ui.historyRunsBody.appendChild(tr);
    });
  }

  async function refreshHistory() {
    if (!ui.historyRunMeta) return;
    const payload = await callApi("/api/history/runs?limit=80");
    const runs = Array.isArray(payload && payload.runs) ? payload.runs : [];
    state.historyRuns = runs;
    ui.historyRunMeta.textContent = `Stored runs: ${payload.total || 0}`;
    renderQuickChips();
    renderHistoryRunsTable();

    if (!runs.length) {
      renderHistorySummary({});
      renderHistoryCases({});
      return;
    }

    const runExists = runs.some((item) => item.run_id === state.selectedHistoryRunId);
    if (!runExists) {
      state.selectedHistoryRunId = runs[0].run_id;
    }
    await selectHistoryRun(state.selectedHistoryRunId);
  }

  function bindActions() {
    if (ui.historyRefreshBtn) {
      ui.historyRefreshBtn.addEventListener("click", () => {
        refreshHistory().catch(() => {});
      });
    }
  }

  function switchPage(name, el) {
    document.querySelectorAll(".page").forEach((page) => page.classList.remove("active"));
    const target = document.getElementById(`page-${name}`);
    if (target) target.classList.add("active");
    document.querySelectorAll(".nav-item").forEach((node) => node.classList.remove("active"));
    if (el) el.classList.add("active");
  }

  function switchConfigTab(el) {
    document.querySelectorAll(".config-nav-item").forEach((node) => node.classList.remove("active"));
    if (el) el.classList.add("active");
  }

  async function boot() {
    bindActions();
    await Promise.all([refreshStatus(), refreshHistory()]);
    setInterval(() => {
      refreshStatus().catch(() => {});
    }, 2000);
    setInterval(() => {
      refreshHistory().catch(() => {});
    }, 20000);
  }

  window.switchPage = switchPage;
  window.switchConfigTab = switchConfigTab;

  boot().catch((err) => {
    if (ui.monitorSubtitle) ui.monitorSubtitle.textContent = `Dashboard init failed: ${err.message}`;
  });
})();
