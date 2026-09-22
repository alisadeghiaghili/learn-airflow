import type { AirflowState, GoalCheck, LevelDef, SolutionStepStatus } from './types';
import { allTasksSuccess, findDag, findTask, hasEdge, latestRun } from './state';

/** Map each solution command to a state-derived completion flag. */
export function solutionProgress(state: AirflowState, solution: string[]): SolutionStepStatus[] {
  return solution.map((command) => stepStatus(state, command));
}

function stepStatus(state: AirflowState, command: string): SolutionStepStatus {
  const cmd = command.trim();
  const fail = (note: string): SolutionStepStatus => ({ command: cmd, done: false, note });
  const ok = (note: string): SolutionStepStatus => ({ command: cmd, done: true, note });

  if (/^airflow\s+db\s+init\b/.test(cmd)) {
    return state.metaInitialized ? ok('metadata initialized') : fail('run `airflow db init`');
  }

  if (/^airflow\s+dags\s+unpause\b/.test(cmd)) {
    const dagId = cmd.split(/\s+/)[3];
    const dag = findDag(state, dagId ?? '');
    if (!dag) return fail(`unpause ${dagId ?? 'dag'}`);
    if (!dag.paused) return ok(`${dag.dag_id} unpaused`);
    // Intermediate unpause that was later re-paused still counts if a run exists.
    if (dag.runs.length) return ok('unpaused earlier (now paused)');
    return fail(`unpause \`${dag.dag_id}\``);
  }

  if (/^airflow\s+dags\s+pause\b/.test(cmd)) {
    const dagId = cmd.split(/\s+/)[3];
    const dag = findDag(state, dagId ?? '');
    if (!dag) return fail(cmd);
    return dag.paused ? ok(`${dag.dag_id} paused`) : fail(`pause \`${dag.dag_id}\``);
  }

  if (/^airflow\s+dags\s+trigger\b/.test(cmd)) {
    const dagId = cmd.split(/\s+/)[3];
    const dag = findDag(state, dagId ?? '');
    if (!dag) return fail(`trigger ${dagId ?? 'dag'}`);
    if (!dag.runs.length) return fail(`trigger \`${dag.dag_id}\``);
    return ok(`${dag.runs.length} run(s)`);
  }

  if (/^airflow\s+dags\s+backfill\b/.test(cmd)) {
    const dagId = cmd.split(/\s+/)[3];
    const dag = findDag(state, dagId ?? '');
    if (!dag) return fail(cmd);
    const backs = dag.runs.filter((r) => r.runType === 'backfill');
    return backs.length ? ok(`${backs.length} backfill run(s)`) : fail('run `airflow dags backfill …`');
  }

  if (/^airflow\s+tasks\s+clear\b/.test(cmd)) {
    // clear reprocesses; done when latest run no longer has the cleared task failed
    // and at least one clear-like recovery happened (all tasks success or task success)
    const parts = cmd.split(/\s+/);
    const dagId = parts[3];
    const tIdx = parts.indexOf('-t');
    const taskId = tIdx >= 0 ? parts[tIdx + 1] : undefined;
    const dag = findDag(state, dagId ?? '');
    if (!dag || !taskId) return fail(cmd);
    const run = latestRun(dag);
    const ti = run?.taskInstances.find((t) => t.task_id === taskId);
    if (ti?.state === 'success') return ok(`${taskId} success after clear`);
    if (ti?.state === 'failed' || ti?.state === 'upstream_failed') {
      return fail(`clear ${taskId} with -y`);
    }
    return fail(`clear ${taskId} with -y`);
  }

  if (/^airflow\s+variables\s+set\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const key = parts[3];
    const value = parts[4];
    if (!key) return fail(cmd);
    const cur = state.variables[key];
    if (cur === undefined) return fail(cmd);
    if (value !== undefined && cur !== value) return fail(cmd);
    return ok(`${key}=${cur}`);
  }

  if (/^airflow\s+connections\s+add\b/.test(cmd)) {
    const connId = cmd.split(/\s+/)[3];
    if (!connId) return fail(cmd);
    return state.connections.some((c) => c.conn_id === connId)
      ? ok(`connection ${connId}`)
      : fail(cmd);
  }

  if (/^dag\s+create\b/.test(cmd)) {
    const dagId = cmd.split(/\s+/)[2];
    const dag = findDag(state, dagId ?? '');
    return dag ? ok(`dag ${dag.dag_id}`) : fail(cmd);
  }

  if (/^dag\s+update\b/.test(cmd)) {
    const dagId = cmd.split(/\s+/)[2];
    const dag = findDag(state, dagId ?? '');
    if (!dag) return fail(cmd);
    const sIdx = cmd.split(/\s+/).indexOf('--schedule');
    if (sIdx >= 0) {
      const raw = (cmd.split(/\s+/)[sIdx + 1] ?? '').replace(/^["']|["']$/g, '');
      const want = raw === 'None' || raw === 'null' || raw === 'none' ? null : raw;
      return dag.schedule === want ? ok(`schedule=${dag.schedule ?? 'None'}`) : fail(cmd);
    }
    if (cmd.includes('--catchup')) {
      return dag.catchup ? ok('catchup=true') : fail(cmd);
    }
    if (cmd.includes('--no-catchup')) {
      return dag.catchup === false ? ok('catchup=false') : fail(cmd);
    }
    return ok('dag updated');
  }

  if (/^task\s+add\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const dagId = parts[2];
    const taskId = parts[3];
    const task = findTask(findDag(state, dagId ?? ''), taskId ?? '');
    return task ? ok(`task ${task.task_id}`) : fail(cmd);
  }

  if (/^task\s+(update|set-retries)\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const dagId = parts[2];
    const taskId = parts[3];
    const task = findTask(findDag(state, dagId ?? ''), taskId ?? '');
    if (!task) return fail(cmd);
    const rIdx = parts.indexOf('--retries');
    if (rIdx >= 0) {
      const n = Number(parts[rIdx + 1] ?? '0');
      return task.retries >= n ? ok(`retries=${task.retries}`) : fail(cmd);
    }
    return ok(`task ${task.task_id} present`);
  }

  if (/^dep\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const dagId = parts[1];
    const chain = cmd
      .slice(cmd.indexOf(parts[1] ?? '') + (parts[1]?.length ?? 0))
      .split('>>')
      .map((s) => s.trim())
      .filter(Boolean);
    const dag = findDag(state, dagId ?? '');
    if (!dag || chain.length < 2) return fail(cmd);
    const all = chain.slice(0, -1).every((from, i) => hasEdge(dag, from, chain[i + 1]!));
    return all ? ok(chain.join(' >> ')) : fail(cmd);
  }

  if (/^airflow\s+dags\s+list\b/.test(cmd) || /^airflow\s+tasks\s+list\b/.test(cmd)) {
    return { command: cmd, done: true, note: 'inspect listing (optional)', optional: true };
  }

  if (/^airflow\s+tasks\s+states-for-dag-run\b/.test(cmd) || /^airflow\s+tasks\s+failed\b/.test(cmd)) {
    return { command: cmd, done: true, note: 'inspect states (optional)', optional: true };
  }

  if (/^airflow\s+dags\s+details\b/.test(cmd) || /^cat\b/.test(cmd) || /^ls\b/.test(cmd)) {
    return { command: cmd, done: true, note: 'inspect (optional)', optional: true };
  }

  if (/^airflow\s+version\b/.test(cmd) || /^airflow\s+scheduler\b/.test(cmd)) {
    return { command: cmd, done: true, note: 'inspect (optional)', optional: true };
  }

  return fail(cmd);
}

export function solutionComplete(state: AirflowState, solution: string[]): boolean {
  return solutionProgress(state, solution).every((s) => s.done || s.optional);
}

export function goalChecklist(state: AirflowState, level: LevelDef): SolutionStepStatus[] {
  return solutionProgress(state, level.solution);
}

export function suggestFromSolution(state: AirflowState, level: LevelDef | null): string | null {
  if (!level) return null;
  const steps = solutionProgress(state, level.solution);
  const next = steps.find((s) => !s.done && !s.optional);
  return next?.command ?? null;
}

export function flattenGoalChecks(goal: GoalCheck): GoalCheck[] {
  return goal.kind === 'allOf' ? goal.checks : [goal];
}

/** True when a successful run exists for the dag (used by tests/helpers). */
export function hasSuccessfulRun(state: AirflowState, dagId: string): boolean {
  return !!findDag(state, dagId)?.runs.some((r) => r.state === 'success');
}

export function latestAllSuccess(state: AirflowState, dagId: string): boolean {
  return allTasksSuccess(findDag(state, dagId), latestRun(findDag(state, dagId)));
}
