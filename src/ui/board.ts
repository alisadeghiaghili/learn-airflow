import type { AirflowState, DagDef, TaskState } from '../engine/types';
import { latestRun } from '../engine/state';
import { renderSvgGraph } from './graph';

function esc(s: string): string {
  return s
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function stateClass(s: TaskState | string): string {
  if (s === 'success') return 'ok';
  if (s === 'running') return 'run';
  if (s === 'failed') return 'fail';
  if (s === 'upstream_failed' || s === 'queued') return 'warn';
  return '';
}

function renderDagFolder(state: AirflowState): string {
  if (!state.metaInitialized) {
    return `<div class="empty-note">AIRFLOW_HOME not initialized — run <code>airflow db init</code>.</div>`;
  }
  if (!state.dags.length) {
    return `<div class="empty-note">No DAGs in <code>dags/</code>. Try <code>dag create my_dag</code>.</div>`;
  }
  const cards = state.dags
    .map((d) => {
      const chips: string[] = [];
      chips.push(
        `<span class="chip ${d.paused ? 'warn' : 'ok'}">${d.paused ? 'paused' : 'unpaused'}</span>`,
      );
      chips.push(`<span class="chip sched">${esc(d.schedule ?? 'None')}</span>`);
      if (d.catchup) chips.push(`<span class="chip">catchup</span>`);
      chips.push(`<span class="chip">${d.tasks.length} tasks</span>`);
      const active = state.activeDagId === d.dag_id ? ' active' : '';
      return `<div class="card${active}" data-dag="${esc(d.dag_id)}">
        <div class="path">${esc(d.dag_id)}</div>
        <div class="meta">${chips.join('')}<span>${esc(d.filePath)}</span></div>
      </div>`;
    })
    .join('');
  return `<div class="cards">${cards}</div>`;
}

function renderGraph(dag: DagDef | undefined): string {
  if (!dag) {
    return `<div class="empty-note">Select or trigger a DAG to see its task graph.</div>`;
  }
  const run = latestRun(dag);
  const stateOf = (taskId: string): TaskState =>
    run?.taskInstances.find((t) => t.task_id === taskId)?.state ?? 'none';

  if (!dag.tasks.length) {
    return `<div class="empty-note">DAG <code>${esc(dag.dag_id)}</code> has no tasks yet.</div>`;
  }

  const nodes = dag.tasks
    .map((t) => {
      const st = stateOf(t.task_id);
      return `<div class="task-node ${stateClass(st)}" data-task="${esc(t.task_id)}">
        <div class="t-id">${esc(t.task_id)}</div>
        <div class="t-op">${esc(t.operator)}</div>
        <div class="t-meta"><span class="chip ${stateClass(st)}">${esc(st)}</span><span>retries=${t.retries}</span></div>
      </div>`;
    })
    .join('');

  const edges = dag.edges.length
    ? dag.edges.map((e) => `<span class="edge">${esc(e.from)} → ${esc(e.to)}</span>`).join('')
    : '<span class="edge muted">no dependencies yet</span>';

  return `<div class="cards">${nodes}</div>
    <div class="edge-list">${edges}</div>`;
}

function renderRuns(dag: DagDef | undefined): string {
  if (!dag || !dag.runs.length) {
    return `<div class="empty-note">No runs yet. Unpause and <code>airflow dags trigger</code>.</div>`;
  }
  const cards = dag.runs
    .slice()
    .reverse()
    .slice(0, 6)
    .map((r) => {
      const tis = r.taskInstances
        .map((ti) => `<span class="chip ${stateClass(ti.state)}">${esc(ti.task_id)}:${esc(ti.state)}</span>`)
        .join('');
      return `<div class="card">
        <div class="path">${esc(r.run_id)}</div>
        <div class="meta">
          <span class="chip ${stateClass(r.state)}">${esc(r.state)}</span>
          <span class="chip">${esc(r.runType)}</span>
          <span>${esc(r.logical_date)}</span>
        </div>
        <div class="meta">${tis}</div>
      </div>`;
    })
    .join('');
  return `<div class="cards">${cards}</div>`;
}

export function renderBoardHtml(state: AirflowState): string {
  const active = state.dags.find((d) => d.dag_id === state.activeDagId) ?? state.dags[0];
  return `
    <div class="status-bar">
      <div class="pill ${state.metaInitialized ? 'ok' : 'err'}">airflow <strong>${state.metaInitialized ? 'ready' : 'not initialized'}</strong></div>
      <div class="pill ${state.schedulerRunning ? 'ok' : 'warn'}">scheduler <strong>${state.schedulerRunning ? 'running' : 'down'}</strong></div>
      <div class="pill">dags <strong>${state.dags.length}</strong></div>
      <div class="pill">connections <strong>${state.connections.length}</strong></div>
      <div class="pill">variables <strong>${Object.keys(state.variables).length}</strong></div>
      <div class="pill">active <strong>${esc(active?.dag_id ?? '—')}</strong></div>
    </div>
    <div class="board">
      <section class="zone dags" aria-label="DAG folder">
        <h2><span class="dot" style="color:var(--airflow)"></span> DAG folder</h2>
        <p class="zone-hint">Parsed DAGs · pause state · schedule</p>
        ${renderDagFolder(state)}
      </section>
      <section class="zone graph" aria-label="Task graph">
        <h2><span class="dot" style="color:var(--run)"></span> Graph</h2>
        <p class="zone-hint">Tasks · operators · dependency edges</p>
        ${renderSvgGraph(active, state)}
      </section>
      <section class="zone runs" aria-label="DAG runs">
        <h2><span class="dot" style="color:var(--sched)"></span> Runs</h2>
        <p class="zone-hint">DagRuns · task instance states</p>
        ${renderRuns(active)}
      </section>
    </div>
    <div class="flow-arrow">scheduler flow: unpause → trigger/schedule → queue → run tasks → success/fail</div>
    ${
      state.connections.length || Object.keys(state.variables).length
        ? `<div class="dag ops-strip">
            <h2>Connections &amp; Variables</h2>
            <div class="stages">
              ${state.connections
                .map(
                  (c) =>
                    `<div class="stage-node"><div><strong>${esc(c.conn_id)}</strong></div><div class="cmd">${esc(c.conn_type)} · ${esc(c.uri)}</div></div>`,
                )
                .join('')}
              ${Object.entries(state.variables)
                .map(
                  ([k, v]) =>
                    `<div class="stage-node"><div><strong>var ${esc(k)}</strong></div><div class="cmd">${esc(v)}</div></div>`,
                )
                .join('')}
            </div>
          </div>`
        : ''
    }
  `;
}
