/**
 * SVG DAG graph — signature visualization (live task-state cascade).
 */

import type { AirflowState, DagDef, TaskState } from '../engine/types';
import { latestRun, topoTasks, upstreamOf } from '../engine/state';

const STATE_FILL: Record<string, string> = {
  none: '#1C2A22',
  queued: '#2A3D32',
  running: '#3DDC97',
  success: '#34D399',
  failed: '#F87171',
  upstream_failed: '#FBBF24',
  skipped: '#8FA898',
};

function esc(s: string): string {
  return s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

export function renderSvgGraph(dag: DagDef | undefined, state: AirflowState): string {
  if (!dag || !dag.tasks.length) {
    return `<div class="empty-note">No DAG graph — create a DAG or load a Python file.</div>`;
  }
  const order = topoTasks(dag);
  const depth = new Map<string, number>();
  for (const t of order) {
    const ups = upstreamOf(dag, t.task_id);
    depth.set(t.task_id, ups.length ? Math.max(...ups.map((u) => (depth.get(u) ?? 0) + 1)) : 0);
  }
  const run = latestRun(dag);
  const tiState = (taskId: string): TaskState =>
    run?.taskInstances.find((t) => t.task_id === taskId)?.state ?? 'none';

  const NODE_W = 132;
  const NODE_H = 52;
  const GAP_X = 56;
  const GAP_Y = 28;
  const PAD = 36;
  const byDepth = new Map<number, string[]>();
  for (const t of order) {
    const d = depth.get(t.task_id) ?? 0;
    if (!byDepth.has(d)) byDepth.set(d, []);
    byDepth.get(d)!.push(t.task_id);
  }
  const maxDepth = Math.max(...byDepth.keys(), 0);
  const maxCol = Math.max(...[...byDepth.values()].map((c) => c.length), 1);
  const pos = new Map<string, { x: number; y: number }>();
  for (const [d, ids] of byDepth) {
    ids.forEach((id, i) => {
      pos.set(id, { x: PAD + d * (NODE_W + GAP_X), y: PAD + i * (NODE_H + GAP_Y) });
    });
  }
  const width = PAD * 2 + (maxDepth + 1) * NODE_W + maxDepth * GAP_X;
  const height = PAD * 2 + maxCol * NODE_H + (maxCol - 1) * GAP_Y;

  const edgesSvg = dag.edges
    .map((e) => {
      const a = pos.get(e.from);
      const b = pos.get(e.to);
      if (!a || !b) return '';
      const x1 = a.x + NODE_W;
      const y1 = a.y + NODE_H / 2;
      const x2 = b.x;
      const y2 = b.y + NODE_H / 2;
      const mx = (x1 + x2) / 2;
      const fromState = tiState(e.from);
      const live = fromState === 'running' || fromState === 'queued';
      const stroke = live ? '#3DDC97' : fromState === 'failed' ? '#F87171' : '#2A3D32';
      return `<path d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}" fill="none" stroke="${stroke}" stroke-width="${live ? 2 : 1}" marker-end="url(#ah)"${live ? ' class="edge-live"' : ''}/>`;
    })
    .join('');

  const nodesSvg = order
    .map((t) => {
      const p = pos.get(t.task_id)!;
      const st = tiState(t.task_id);
      const fill = STATE_FILL[st] ?? '#1C2A22';
      const stroke = st === 'running' ? '#3DDC97' : st === 'failed' ? '#F87171' : '#2A3D32';
      const cls =
        st === 'running' ? ' class="ti-running"' : st === 'failed' ? ' class="ti-failed"' : '';
      return `<g${cls}><rect x="${p.x}" y="${p.y}" width="${NODE_W}" height="${NODE_H}" rx="10" fill="${fill}" stroke="${stroke}" stroke-width="1.5"/><text x="${p.x + NODE_W / 2}" y="${p.y + 18}" text-anchor="middle" font-size="12" font-weight="600" fill="#E6F0EA">${esc(t.task_id)}</text><text x="${p.x + NODE_W / 2}" y="${p.y + 36}" text-anchor="middle" font-size="10" fill="#8FA898">${esc(t.operator)} · ${esc(st)}</text></g>`;
    })
    .join('');

  void state;
  return `<svg class="dag-svg" viewBox="0 0 ${width} ${height}" width="100%" height="${Math.min(height, 320)}" role="img" aria-label="DAG graph for ${esc(dag.dag_id)}"><defs><marker id="ah" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0,0 L8,5 L0,10" fill="none" stroke="context-stroke" stroke-width="1.2"/></marker></defs><rect width="${width}" height="${height}" fill="#0C1410" rx="8"/>${edgesSvg}${nodesSvg}</svg><style>.dag-svg{display:block;border:1px solid #2A3D32;border-radius:8px;background:#0C1410}.ti-running rect{animation:tiPulse 1.2s ease-in-out infinite}.ti-failed rect{animation:tiFail 1.4s ease-in-out infinite}.edge-live{stroke-dasharray:6 4;animation:edgeFlow 1s linear infinite}@keyframes tiPulse{0%,100%{opacity:1}50%{opacity:.75}}@keyframes tiFail{0%,100%{filter:none}50%{filter:brightness(1.25)}}@keyframes edgeFlow{to{stroke-dashoffset:-20}}@media (prefers-reduced-motion:reduce){.ti-running rect,.ti-failed rect,.edge-live{animation:none}}</style>`;
}
