import type { AirflowState, GoalCheck, LevelDef } from './types';
import { evaluateGoal } from './compare';
import { solutionProgress, suggestFromSolution } from './solution';

export interface NextStep {
  label: string;
  command: string | null;
}

/** Remaining work authored from the level solution itself. */
export function nextSteps(
  state: AirflowState,
  goal: GoalCheck,
  level?: LevelDef | null,
): NextStep[] {
  if (level?.solution?.length) {
    return solutionProgress(state, level.solution)
      .filter((s) => !s.done && !s.optional)
      .map((s) => ({ label: s.note, command: s.command }));
  }
  const { statuses } = evaluateGoal(state, goal);
  const steps: NextStep[] = [];
  statuses.forEach((st, i) => {
    if (st.met) return;
    const cmd = suggestFromSolution(state, (level ?? null) as LevelDef | null) ?? (st.command ?? null);
    steps.push({ label: st.label, command: i === 0 ? cmd : null });
  });
  return steps;
}

export function formatNextSteps(steps: NextStep[], level: LevelDef | null): string {
  if (!steps.length) {
    return 'All goal steps are met — you should be done. Type `show goal` to confirm.';
  }
  const lines = [`Next steps (${steps.length} remaining)${level ? ` for ${level.id}` : ''}:`];
  steps.forEach((s, i) => {
    const cmd = s.command ? `\n      ${s.command}` : '';
    lines.push(`  ${i + 1}. [ ] ${s.label}${cmd}`);
  });
  lines.push('Type `steps` to repeat this list · `hint` · `show goal`');
  return lines.join('\n');
}

export function coachLine(state: AirflowState, level: LevelDef | null): string | null {
  if (!level) return null;
  if (level.solution.length && solutionProgress(state, level.solution).every((s) => s.done || s.optional)) {
    return null;
  }
  const { solved } = evaluateGoal(state, level.goal);
  if (solved && !level.solution.length) return null;
  return formatNextSteps(nextSteps(state, level.goal, level), level);
}
