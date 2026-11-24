/**
 * DISTASKENG Topology Dashboard - Real-time Visualization Engine
 * Vanilla JS + D3.js v7
 */

(function () {
  "use strict";

  // State
  let currentEtag = null;
  let isPolling = false;
  let snapshotData = null;
  let selectedNodeId = null;
  let selectedNodeType = null; // 'job' | 'worker' | 'hub'
  let activeStatusFilter = "ALL";
  let activeTypeFilter = "ALL";
  let activePriorityFilter = "ALL";
  let activeSearchQuery = "";
  const knownJobTypes = new Set();

  // D3 Elements & Simulation
  let svg, gZoom, gEdges, gNodes, simulation, zoomBehavior;
  let nodeMap = new Map(); // id -> node object with {x, y, vx, vy}
  let linkMap = new Map(); // id -> link object

  // DOM Elements
  const elConnIndicator = document.getElementById("conn-indicator");
  const elSyncTag = document.getElementById("sync-tag");
  const elSyncTime = document.getElementById("sync-time");

  const elQueueDepth = document.getElementById("val-queue-depth");
  const elQueueBreakdown = document.getElementById("val-queue-breakdown");
  const elThroughput = document.getElementById("val-throughput");
  const elLatency = document.getElementById("val-latency");
  const elFailureRate = document.getElementById("val-failure-rate");
  const elActiveWorkers = document.getElementById("val-active-workers");
  const elTotalWorkers = document.getElementById("val-total-workers");
  const elVisibleNodes = document.getElementById("val-visible-nodes");

  const elTypeSelect = document.getElementById("type-select");
  const elPrioritySelect = document.getElementById("priority-select");
  const elSearchInput = document.getElementById("search-input");
  const elClearSearch = document.getElementById("clear-search");
  const elStatusPills = document.getElementById("status-pills");
  const elRecenter = document.getElementById("btn-recenter");

  const elEmptyState = document.getElementById("empty-state");
  const elInspectorPanel = document.getElementById("inspector-panel");
  const elCloseInspector = document.getElementById("close-inspector");
  const elInspectorEmpty = document.querySelector(".inspector-empty");
  const elInspectorFields = document.getElementById("inspector-fields");

  const elInspTypeBadge = document.getElementById("insp-type-badge");
  const elInspStatusPill = document.getElementById("insp-status-pill");
  const elInspId = document.querySelector("#insp-id span");
  const elBtnCopyId = document.getElementById("btn-copy-id");
  const elInspJobType = document.getElementById("insp-job-type");
  const elInspPriority = document.getElementById("insp-priority");
  const elInspWorker = document.getElementById("insp-worker");
  const elInspDuration = document.getElementById("insp-duration");
  const elInspAttemptCount = document.getElementById("insp-attempt-count");
  const elInspMaxRetries = document.getElementById("insp-max-retries");
  const elInspAttemptsList = document.getElementById("insp-attempts-list");
  const elInspParentChips = document.getElementById("insp-parent-chips");
  const elInspChildChips = document.getElementById("insp-child-chips");
  const elInspResultBox = document.getElementById("insp-result-box");
  const elInspErrorBox = document.getElementById("insp-error-box");
  const elSectionError = document.getElementById("section-error");
  const elInspTsCreated = document.getElementById("insp-ts-created");
  const elInspTsStarted = document.getElementById("insp-ts-started");
  const elInspTsFinished = document.getElementById("insp-ts-finished");

  const elEventFeedList = document.getElementById("event-feed-list");
  const elEventCount = document.getElementById("event-count");
  const elChkAutoScroll = document.getElementById("chk-autoscroll");
  const elBtnClearFeed = document.getElementById("btn-clear-feed");

  // Status color map
  const STATUS_COLORS = {
    QUEUED: "#9CA3A3",
    RUNNING: "#5EEAD4",
    SUCCEEDED: "#4ADE80",
    FAILED: "#FBBF24",
    DEAD: "#F87171",
    PENDING: "#7F8178",
    CANCELLED: "#6B7280"
  };

  /**
   * Initialize Graph SVG, Zoom, Markers, and Force Simulation
   */
  function initGraph() {
    const container = document.getElementById("graph-container");
    const width = container.clientWidth || 1000;
    const height = container.clientHeight || 700;

    svg = d3.select("#graph-svg")
      .attr("width", "100%")
      .attr("height", "100%")
      .attr("viewBox", [0, 0, width, height]);

    // Defs for arrows and glows
    const defs = svg.append("defs");

    // Arrow markers for DAG dependencies
    const createMarker = (id, color) => {
      defs.append("marker")
        .attr("id", id)
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 18)
        .attr("refY", 0)
        .attr("markerWidth", 6)
        .attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M0,-5L10,0L0,5")
        .attr("fill", color);
    };

    createMarker("arrow-dep-satisfied", "#4ADE80");
    createMarker("arrow-dep-waiting", "#7F8178");
    createMarker("arrow-dep-blocked", "#F87171");

    // Zoom container
    gZoom = svg.append("g").attr("class", "zoom-root");
    gEdges = gZoom.append("g").attr("class", "layer-edges");
    gNodes = gZoom.append("g").attr("class", "layer-nodes");

    zoomBehavior = d3.zoom()
      .scaleExtent([0.2, 4])
      .on("zoom", (event) => {
        gZoom.attr("transform", event.transform);
      });

    svg.call(zoomBehavior).on("dblclick.zoom", null);

    // Initialize Simulation
    simulation = d3.forceSimulation()
      .force("link", d3.forceLink().id(d => d.id).distance(d => {
        if (d.edge_type === "queue") return 90;
        if (d.edge_type === "assignment") return 65;
        if (d.edge_type === "dependency") return 110;
        return 80;
      }).strength(d => {
        if (d.edge_type === "assignment") return 0.7;
        if (d.edge_type === "dependency") return 0.4;
        return 0.3;
      }))
      .force("charge", d3.forceManyBody().strength(d => {
        if (d.type === "hub") return -800;
        if (d.type === "worker") return -350;
        return -80;
      }))
      .force("collide", d3.forceCollide().radius(d => {
        if (d.type === "hub") return 60;
        if (d.type === "worker") return 36;
        return 14;
      }))
      .force("center", d3.forceCenter(width / 2, height / 2).strength(0.08))
      .on("tick", ticked);

    // Handle Window Resize
    window.addEventListener("resize", () => {
      const w = container.clientWidth;
      const h = container.clientHeight;
      simulation.force("center", d3.forceCenter(w / 2, h / 2));
      simulation.alpha(0.2).restart();
    });
  }

  /**
   * Simulation Tick Handler
   */
  function ticked() {
    gEdges.selectAll(".graph-edge")
      .attr("x1", d => d.source.x)
      .attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x)
      .attr("y2", d => d.target.y);

    gNodes.selectAll(".graph-node")
      .attr("transform", d => `translate(${d.x},${d.y})`);
  }

  /**
   * Drag behaviors for graph nodes
   */
  function drag(simulation) {
    function dragstarted(event, d) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    }

    function dragged(event, d) {
      d.fx = event.x;
      d.fy = event.y;
    }

    function dragended(event, d) {
      if (!event.active) simulation.alphaTarget(0);
      if (d.type !== "hub") {
        d.fx = null;
        d.fy = null;
      }
    }

    return d3.drag()
      .on("start", dragstarted)
      .on("drag", dragged)
      .on("end", dragended);
  }

  /**
   * Format numbers & durations
   */
  function formatDuration(ms) {
    if (ms === null || ms === undefined) return "-";
    if (ms < 1000) return `${Math.round(ms)}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  }

  function formatTimestamp(isoStr) {
    if (!isoStr) return "-";
    try {
      const d = new Date(isoStr);
      return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", fractionDigits: 3 });
    } catch {
      return isoStr;
    }
  }

  /**
   * Update Metrics Strip
   */
  function updateTopMetrics(stats, counts, hub) {
    if (!stats) return;

    elQueueDepth.textContent = hub.queue_depth || stats.queue_depth || 0;
    elQueueBreakdown.textContent = `${hub.delayed_depth || 0} delayed \u2022 ${hub.dlq_depth || 0} dlq`;

    elThroughput.textContent = Number(stats.jobs_per_second || 0).toFixed(1);
    elLatency.textContent = `${Math.round(stats.p50_latency_ms || 0)} / ${Math.round(stats.p95_latency_ms || 0)} / ${Math.round(stats.p99_latency_ms || 0)}`;

    const failRate = Number(stats.failure_rate_percent || 0);
    elFailureRate.textContent = `${failRate.toFixed(1)}%`;
    if (failRate > 10) {
      elFailureRate.className = "metric-value text-accent-red";
    } else if (failRate > 0) {
      elFailureRate.className = "metric-value text-accent-amber";
    } else {
      elFailureRate.className = "metric-value text-accent-green";
    }

    elActiveWorkers.textContent = stats.active_workers || 0;
    elTotalWorkers.textContent = `/ ${stats.total_workers || 0} alive`;
  }

  /**
   * Filter predicate for a job node
   */
  function passesFilter(job) {
    if (activeStatusFilter !== "ALL" && job.status !== activeStatusFilter) {
      return false;
    }
    if (activeTypeFilter !== "ALL" && job.job_type !== activeTypeFilter) {
      return false;
    }
    if (activePriorityFilter !== "ALL") {
      const minPrio = parseInt(activePriorityFilter, 10);
      if (job.priority < minPrio) return false;
    }
    if (activeSearchQuery.trim()) {
      const q = activeSearchQuery.toLowerCase().trim();
      const matchId = job.id.toLowerCase().includes(q);
      const matchType = job.job_type.toLowerCase().includes(q);
      const matchWorker = job.worker_id && job.worker_id.toLowerCase().includes(q);
      const matchError = job.error && job.error.toLowerCase().includes(q);
      if (!matchId && !matchType && !matchWorker && !matchError) return false;
    }
    return true;
  }

  /**
   * Render D3 Topology
   */
  function renderTopology(snapshot) {
    const container = document.getElementById("graph-container");
    const width = container.clientWidth || 1000;
    const height = container.clientHeight || 700;

    // Build unified nodes array
    const rawNodes = [];

    // 1. Hub node (fixed near center)
    const hubNode = {
      id: "hub",
      type: "hub",
      queue_depth: snapshot.hub.queue_depth,
      delayed_depth: snapshot.hub.delayed_depth,
      dlq_depth: snapshot.hub.dlq_depth,
      fx: width / 2,
      fy: height / 2
    };
    rawNodes.push(hubNode);

    // 2. Worker nodes
    const workerCount = snapshot.workers.length;
    snapshot.workers.forEach((w, idx) => {
      // Place workers in orbit around center if new
      const angle = (idx / Math.max(workerCount, 1)) * 2 * Math.PI;
      const radius = 175;
      const wx = width / 2 + radius * Math.cos(angle);
      const wy = height / 2 + radius * Math.sin(angle);

      rawNodes.push({
        id: w.id,
        type: "worker",
        data: w,
        defaultX: wx,
        defaultY: wy
      });
    });

    // 3. Job nodes (applying filters)
    let visibleJobsCount = 0;
    snapshot.jobs.forEach(job => {
      if (passesFilter(job)) {
        visibleJobsCount++;
        rawNodes.push({
          id: job.id,
          type: "job",
          data: job,
          status: job.status,
          priority: job.priority
        });
      }
    });

    elVisibleNodes.textContent = visibleJobsCount;

    // Toggle Empty State banner
    if (snapshot.jobs.length === 0) {
      elEmptyState.classList.remove("hidden");
    } else {
      elEmptyState.classList.add("hidden");
    }

    // Merge nodeMap to preserve positions
    const activeNodes = rawNodes.map(raw => {
      const existing = nodeMap.get(raw.id);
      if (existing) {
        // Retain current coordinates and velocity
        raw.x = existing.x;
        raw.y = existing.y;
        raw.vx = existing.vx;
        raw.vy = existing.vy;
      } else if (raw.defaultX !== undefined) {
        raw.x = raw.defaultX;
        raw.y = raw.defaultY;
      } else if (raw.type === "job") {
        // Spawn jobs near hub or near assigned worker
        const assignedWorker = raw.data && raw.data.worker_id && nodeMap.get(raw.data.worker_id);
        const parentPos = assignedWorker || nodeMap.get("hub") || { x: width / 2, y: height / 2 };
        raw.x = parentPos.x + (Math.random() - 0.5) * 50;
        raw.y = parentPos.y + (Math.random() - 0.5) * 50;
      }
      return raw;
    });

    // Update nodeMap cache
    nodeMap = new Map(activeNodes.map(n => [n.id, n]));

    // Filter edges whose source and target are both currently active nodes
    const validEdges = [];
    snapshot.edges.forEach(e => {
      if (nodeMap.has(e.source) && nodeMap.has(e.target)) {
        validEdges.push({
          id: `${e.source}->${e.target}:${e.edge_type}`,
          source: e.source,
          target: e.target,
          edge_type: e.edge_type,
          status: e.status,
          priority: e.priority || 5
        });
      }
    });

    // =========================================================================
    // D3 Data Join: EDGES
    // =========================================================================
    const edgeSelection = gEdges.selectAll(".graph-edge")
      .data(validEdges, d => d.id);

    edgeSelection.exit().remove();

    const edgeEnter = edgeSelection.enter()
      .append("line")
      .attr("class", d => {
        let cls = `graph-edge edge-${d.edge_type}`;
        if (d.edge_type === "assignment") cls += " edge-running-flow";
        if (d.edge_type === "dependency") {
          if (d.status === "blocked_failed") cls += " edge-dependency-blocked";
          else if (d.status === "waiting") cls += " edge-dependency-waiting";
        }
        return cls;
      })
      .attr("marker-end", d => {
        if (d.edge_type === "dependency") {
          if (d.status === "blocked_failed") return "url(#arrow-dep-blocked)";
          if (d.status === "waiting") return "url(#arrow-dep-waiting)";
          return "url(#arrow-dep-satisfied)";
        }
        return null;
      });

    // =========================================================================
    // D3 Data Join: NODES
    // =========================================================================
    const nodeSelection = gNodes.selectAll(".graph-node")
      .data(activeNodes, d => d.id);

    nodeSelection.exit().remove();

    const nodeEnter = nodeSelection.enter()
      .append("g")
      .attr("class", d => `graph-node node-${d.type}`)
      .call(drag(simulation))
      .on("click", (event, d) => {
        event.stopPropagation();
        inspectNode(d);
      });

    // 1. Hub Node Elements
    const hubEnter = nodeEnter.filter(d => d.type === "hub");
    hubEnter.append("circle")
      .attr("class", "hub-outer")
      .attr("r", 46);
    hubEnter.append("circle")
      .attr("class", "hub-core")
      .attr("r", 34);
    hubEnter.append("text")
      .attr("class", "node-label")
      .attr("dy", "-6")
      .text("QUEUE HUB");
    hubEnter.append("text")
      .attr("class", "node-label mono hub-depth-text")
      .attr("dy", "14")
      .attr("font-size", "14px")
      .attr("font-weight", "bold")
      .attr("fill", "#5EEAD4")
      .text(d => d.queue_depth || 0);

    // 2. Worker Node Elements
    const workerEnter = nodeEnter.filter(d => d.type === "worker");
    workerEnter.append("circle")
      .attr("class", "worker-ring")
      .attr("r", d => 20 + Math.min((d.data.load_score || 0) * 12, 10))
      .attr("stroke", d => {
        if (!d.data.is_alive) return STATUS_COLORS.DEAD;
        if (d.data.heartbeat_age_seconds > 15) return STATUS_COLORS.FAILED;
        return STATUS_COLORS.SUCCEEDED;
      });
    workerEnter.append("text")
      .attr("class", "node-label")
      .attr("dy", "3")
      .text(d => {
        const id = d.id;
        return id.length > 10 ? `${id.slice(0, 8)}...` : id;
      });
    workerEnter.append("text")
      .attr("class", "node-label node-label-sub mono")
      .attr("dy", "15")
      .text(d => `${d.data.running_job_count || 0}/${d.data.concurrency || 4}`);

    // 3. Job Node Elements
    const jobEnter = nodeEnter.filter(d => d.type === "job");
    jobEnter.append("circle")
      .attr("class", "job-core")
      .attr("r", d => 7 + (d.priority >= 7 ? 2 : 0))
      .attr("fill", d => STATUS_COLORS[d.status] || "#9CA3A3")
      .attr("opacity", d => (["SUCCEEDED", "DEAD"].includes(d.status) ? 0.6 : 1.0));

    // Append hover title for native quick tooltip
    jobEnter.append("title")
      .text(d => `Job: ${d.id}\nType: ${d.data.job_type}\nStatus: ${d.status}\nPriority: ${d.priority}`);

    // =========================================================================
    // UPDATE EXISTING NODES
    // =========================================================================
    const nodeUpdate = nodeSelection.merge(nodeEnter);

    // Update Hub depth
    nodeUpdate.filter(d => d.type === "hub")
      .select(".hub-depth-text")
      .text(d => d.queue_depth || 0);

    // Update Worker rings & labels
    nodeUpdate.filter(d => d.type === "worker").each(function (d) {
      const g = d3.select(this);
      g.select(".worker-ring")
        .attr("r", 20 + Math.min((d.data.load_score || 0) * 12, 10))
        .attr("stroke", () => {
          if (!d.data.is_alive) return STATUS_COLORS.DEAD;
          if (d.data.heartbeat_age_seconds > 15) return STATUS_COLORS.FAILED;
          return STATUS_COLORS.SUCCEEDED;
        });
      g.select(".node-label-sub")
        .text(`${d.data.running_job_count || 0}/${d.data.concurrency || 4}`);
    });

    // Update Job circles
    nodeUpdate.filter(d => d.type === "job").each(function (d) {
      const g = d3.select(this);
      g.select(".job-core")
        .attr("fill", STATUS_COLORS[d.status] || "#9CA3A3")
        .attr("opacity", ["SUCCEEDED", "DEAD"].includes(d.status) ? 0.6 : 1.0);
      g.select("title")
        .text(`Job: ${d.id}\nType: ${d.data.job_type}\nStatus: ${d.status}\nPriority: ${d.priority}`);
    });

    // Highlight selected node
    nodeUpdate.classed("selected", d => d.id === selectedNodeId);

    // Restart simulation
    simulation.nodes(activeNodes);
    simulation.force("link").links(validEdges);
    simulation.alpha(0.2).restart();
  }

  /**
   * Inspector Panel: Inspect selected node
   */
  function inspectNode(node) {
    if (!node) return;
    selectedNodeId = node.id;
    selectedNodeType = node.type;

    // Highlight selected in graph
    gNodes.selectAll(".graph-node").classed("selected", d => d.id === selectedNodeId);

    elInspectorEmpty.classList.add("hidden");
    elInspectorFields.classList.remove("hidden");
    elInspectorPanel.classList.remove("collapsed");

    if (node.type === "job") {
      const job = node.data;
      elInspTypeBadge.textContent = "JOB";
      elInspStatusPill.textContent = job.status;
      elInspStatusPill.className = `panel-status-pill status-${job.status}`;

      elInspId.textContent = job.id;
      elInspJobType.textContent = job.job_type;
      elInspPriority.textContent = `${job.priority} / 9`;
      elInspWorker.textContent = job.worker_id || "Unassigned (Queued)";
      elInspDuration.textContent = formatDuration(job.execution_duration_ms);

      elInspAttemptCount.textContent = job.attempts_count || 0;
      elInspMaxRetries.textContent = job.max_retries || 0;

      // Render Attempts
      elInspAttemptsList.innerHTML = "";
      if (job.attempts && job.attempts.length > 0) {
        job.attempts.forEach(att => {
          const row = document.createElement("div");
          row.className = "attempt-row mono";
          row.innerHTML = `
            <span>#${att.attempt_number} \u2022 ${att.worker_id || "worker"}</span>
            <span class="attempt-tag tag-${att.status}">${att.status}</span>
            <span>${formatDuration(att.duration_ms)}</span>
          `;
          elInspAttemptsList.appendChild(row);
        });
      } else {
        elInspAttemptsList.innerHTML = `<span class="text-muted">No attempts recorded yet</span>`;
      }

      // Render Dependencies
      elInspParentChips.innerHTML = "";
      if (job.depends_on && job.depends_on.length > 0) {
        job.depends_on.forEach(pId => {
          const chip = document.createElement("span");
          chip.className = "dep-chip mono";
          chip.textContent = pId.slice(0, 8);
          chip.title = pId;
          chip.addEventListener("click", () => {
            const parentNode = nodeMap.get(pId);
            if (parentNode) inspectNode(parentNode);
          });
          elInspParentChips.appendChild(chip);
        });
      } else {
        elInspParentChips.innerHTML = `<span class="text-muted">None (root)</span>`;
      }

      elInspChildChips.innerHTML = "";
      if (job.blocked_children && job.blocked_children.length > 0) {
        job.blocked_children.forEach(cId => {
          const chip = document.createElement("span");
          chip.className = "dep-chip mono";
          chip.textContent = cId.slice(0, 8);
          chip.title = cId;
          chip.addEventListener("click", () => {
            const childNode = nodeMap.get(cId);
            if (childNode) inspectNode(childNode);
          });
          elInspChildChips.appendChild(chip);
        });
      } else {
        elInspChildChips.innerHTML = `<span class="text-muted">None (leaf)</span>`;
      }

      // Result payload
      if (job.result !== null && job.result !== undefined) {
        elInspResultBox.textContent = JSON.stringify(job.result, null, 2);
      } else {
        elInspResultBox.textContent = "null";
      }

      // Error trace
      if (job.error) {
        elSectionError.classList.remove("hidden");
        elInspErrorBox.textContent = job.error;
      } else {
        elSectionError.classList.add("hidden");
      }

      // Timestamps
      elInspTsCreated.textContent = formatTimestamp(job.created_at);
      elInspTsStarted.textContent = formatTimestamp(job.started_at);
      elInspTsFinished.textContent = formatTimestamp(job.completed_at);
    } else if (node.type === "worker") {
      const w = node.data;
      elInspTypeBadge.textContent = "WORKER";
      elInspStatusPill.textContent = w.is_alive ? "ONLINE" : "DEAD";
      elInspStatusPill.className = `panel-status-pill status-${w.is_alive ? "SUCCEEDED" : "DEAD"}`;

      elInspId.textContent = w.id;
      elInspJobType.textContent = `Concurrency: ${w.concurrency}`;
      elInspPriority.textContent = `Load: ${((w.load_score || 0) * 100).toFixed(0)}%`;
      elInspWorker.textContent = `Running: ${w.running_job_count}`;
      elInspDuration.textContent = `Last HB: ${w.heartbeat_age_seconds ? w.heartbeat_age_seconds.toFixed(1) + "s ago" : "-"}`;

      elInspAttemptsList.innerHTML = `<span class="text-muted">Worker node</span>`;
      elInspParentChips.innerHTML = `<span class="text-muted">-</span>`;
      elInspChildChips.innerHTML = `<span class="text-muted">-</span>`;
      elInspResultBox.textContent = JSON.stringify({
        cpu_percent: w.cpu_percent,
        memory_percent: w.memory_percent,
        is_alive: w.is_alive
      }, null, 2);
      elSectionError.classList.add("hidden");

      elInspTsCreated.textContent = formatTimestamp(w.registered_at);
      elInspTsStarted.textContent = formatTimestamp(w.last_heartbeat_at);
      elInspTsFinished.textContent = "-";
    } else if (node.type === "hub") {
      elInspTypeBadge.textContent = "SCHEDULER";
      elInspStatusPill.textContent = "HUB";
      elInspStatusPill.className = "panel-status-pill status-RUNNING";
      elInspId.textContent = "hub";
      elInspJobType.textContent = "Redis Priority Queue";
      elInspPriority.textContent = "Priorities 0-9";
      elInspWorker.textContent = "-";
      elInspDuration.textContent = "-";
      elInspAttemptsList.innerHTML = "";
      elInspParentChips.innerHTML = `<span class="text-muted">-</span>`;
      elInspChildChips.innerHTML = `<span class="text-muted">-</span>`;
      elInspResultBox.textContent = JSON.stringify({
        queue_depth: node.queue_depth,
        delayed_depth: node.delayed_depth,
        dlq_depth: node.dlq_depth
      }, null, 2);
      elSectionError.classList.add("hidden");
      elInspTsCreated.textContent = "-";
      elInspTsStarted.textContent = "-";
      elInspTsFinished.textContent = "-";
    }
  }

  /**
   * Render Event Stream (Last 50 Transitions)
   */
  function renderEvents(events) {
    if (!events || events.length === 0) return;
    elEventCount.textContent = `${events.length} TRANSITIONS`;
    elEventFeedList.innerHTML = "";

    events.forEach(ev => {
      const row = document.createElement("div");
      row.className = "feed-row";
      const shortId = (ev.job_id || ev.worker_id || "-").slice(0, 10);
      const timeStr = formatTimestamp(ev.timestamp);

      row.innerHTML = `
        <span class="feed-time">${timeStr}</span>
        <div><span class="feed-event-tag tag-${ev.event_type}">${ev.event_type}</span></div>
        <span class="feed-target" title="${ev.job_id || ev.worker_id}">${shortId}</span>
        <span class="feed-detail" title="${ev.details || ""}">${ev.details || "-"}</span>
      `;

      row.addEventListener("click", () => {
        if (ev.job_id && nodeMap.has(ev.job_id)) {
          inspectNode(nodeMap.get(ev.job_id));
        } else if (ev.worker_id && nodeMap.has(ev.worker_id)) {
          inspectNode(nodeMap.get(ev.worker_id));
        }
      });

      elEventFeedList.appendChild(row);
    });

    if (elChkAutoScroll.checked) {
      elEventFeedList.scrollTop = elEventFeedList.scrollHeight;
    }
  }

  /**
   * Update Job Types in Filter Dropdown
   */
  function updateJobTypeOptions(jobs) {
    let changed = false;
    jobs.forEach(j => {
      if (j.job_type && !knownJobTypes.has(j.job_type)) {
        knownJobTypes.add(j.job_type);
        changed = true;
      }
    });

    if (changed) {
      const currentVal = elTypeSelect.value;
      elTypeSelect.innerHTML = `<option value="ALL">ALL TYPES</option>`;
      Array.from(knownJobTypes).sort().forEach(t => {
        const opt = document.createElement("option");
        opt.value = t;
        opt.textContent = t;
        if (t === currentVal) opt.selected = true;
        elTypeSelect.appendChild(opt);
      });
    }
  }

  /**
   * Poll Dashboard Snapshot Endpoint
   */
  async function pollSnapshot() {
    if (isPolling) return;
    isPolling = true;

    try {
      const headers = { "Accept": "application/json" };
      if (currentEtag) {
        headers["If-None-Match"] = currentEtag;
      }

      const res = await fetch("/api/v1/dashboard/snapshot", { headers });

      const now = new Date();
      elSyncTime.textContent = now.toLocaleTimeString();

      if (res.status === 304) {
        // Not modified: pulse indicator green
        elConnIndicator.className = "pulse-indicator";
        elSyncTag.textContent = "IN SYNC (304)";
      } else if (res.status === 200) {
        currentEtag = res.headers.get("ETag") || currentEtag;
        const data = await res.json();
        snapshotData = data;

        elConnIndicator.className = "pulse-indicator";
        elSyncTag.textContent = "POLLING (1s)";

        // Update metrics
        updateTopMetrics(data.stats, data.status_counts, data.hub);
        updateJobTypeOptions(data.jobs);

        // Render Graph
        renderTopology(data);

        // Render Events
        renderEvents(data.events);

        // If selected node is still in snapshot, re-render its details
        if (selectedNodeId && nodeMap.has(selectedNodeId)) {
          inspectNode(nodeMap.get(selectedNodeId));
        }
      } else {
        elConnIndicator.className = "pulse-indicator stale";
        elSyncTag.textContent = `HTTP ${res.status}`;
      }
    } catch (err) {
      console.warn("Snapshot fetch failed:", err);
      elConnIndicator.className = "pulse-indicator error";
      elSyncTag.textContent = "OFFLINE";
    } finally {
      isPolling = false;
    }
  }

  /**
   * Recenter and reset zoom
   */
  function recenterGraph() {
    if (!svg || !zoomBehavior) return;
    svg.transition().duration(500).call(
      zoomBehavior.transform,
      d3.zoomIdentity
    );
  }

  /**
   * Setup Event Listeners
   */
  function setupEventListeners() {
    // Status Filter Pills
    elStatusPills.querySelectorAll(".pill").forEach(pill => {
      pill.addEventListener("click", () => {
        elStatusPills.querySelectorAll(".pill").forEach(p => p.classList.remove("active"));
        pill.classList.add("active");
        activeStatusFilter = pill.getAttribute("data-status");
        if (snapshotData) renderTopology(snapshotData);
      });
    });

    // Job Type Filter
    elTypeSelect.addEventListener("change", (e) => {
      activeTypeFilter = e.target.value;
      if (snapshotData) renderTopology(snapshotData);
    });

    // Priority Filter
    elPrioritySelect.addEventListener("change", (e) => {
      activePriorityFilter = e.target.value;
      if (snapshotData) renderTopology(snapshotData);
    });

    // Search Input
    elSearchInput.addEventListener("input", (e) => {
      activeSearchQuery = e.target.value;
      if (snapshotData) renderTopology(snapshotData);
    });

    elClearSearch.addEventListener("click", () => {
      elSearchInput.value = "";
      activeSearchQuery = "";
      if (snapshotData) renderTopology(snapshotData);
    });

    // Recenter
    elRecenter.addEventListener("click", recenterGraph);

    // Inspector Close
    elCloseInspector.addEventListener("click", () => {
      elInspectorPanel.classList.add("collapsed");
      selectedNodeId = null;
      selectedNodeType = null;
      gNodes.selectAll(".graph-node").classed("selected", false);
    });

    // Copy ID button
    elBtnCopyId.addEventListener("click", () => {
      const text = elInspId.textContent;
      if (text && text !== "-") {
        navigator.clipboard.writeText(text);
        elBtnCopyId.textContent = "COPIED!";
        setTimeout(() => {
          elBtnCopyId.textContent = "COPY";
        }, 1200);
      }
    });

    // Clear feed
    elBtnClearFeed.addEventListener("click", () => {
      elEventFeedList.innerHTML = `<div class="feed-empty">Event stream cleared.</div>`;
    });

    // Click outside to deselect
    document.getElementById("graph-svg").addEventListener("click", () => {
      selectedNodeId = null;
      selectedNodeType = null;
      gNodes.selectAll(".graph-node").classed("selected", false);
      elInspectorEmpty.classList.remove("hidden");
      elInspectorFields.classList.add("hidden");
    });
  }

  /**
   * Main Initialization
   */
  function init() {
    initGraph();
    setupEventListeners();

    // Initial snapshot pull
    pollSnapshot();

    // 1-second polling interval
    setInterval(pollSnapshot, 1000);
  }

  // Run when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
