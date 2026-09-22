import { describe, expect, it } from 'vitest';
import { emptyState } from '../src/engine/state';
import { executeCommand } from '../src/engine/commands';
import { solutionComplete, solutionProgress } from '../src/engine/solution';
import { getLevel, allLevels } from '../src/levels';

describe('solution checklist mirrors goals', () => {
  it('intro-1 checklist: db init', () => {
    const level = getLevel('intro-1');
    expect(level).toBeTruthy();
    expect(level!.solution).toEqual(['airflow db init']);

    let state = emptyState();
    let steps = solutionProgress(state, level!.solution);
    expect(steps[0].done).toBe(false);

    state = executeCommand(state, 'airflow db init').state;
    steps = solutionProgress(state, level!.solution);
    expect(steps[0].done).toBe(true);
    expect(solutionComplete(state, level!.solution)).toBe(true);
  });

  it('every level solution command appears in the checklist UI list', () => {
    for (const level of allLevels) {
      const steps = solutionProgress(level.startState, level.solution);
      expect(steps.map((s) => s.command)).toEqual(level.solution);
    }
  });

  it('every LGB-style level has a goalVisual target', () => {
    for (const level of allLevels) {
      expect(level.goalVisual, `missing goalVisual on ${level.id}`).toBeTruthy();
    }
  });

  it('level solutions complete their checklists', () => {
    for (const level of allLevels) {
      let state = structuredClone(level.startState);
      for (const cmd of level.solution) {
        const step = executeCommand(state, cmd);
        if (step.result.error) {
          throw new Error(`${level.id}: ${cmd} → ${step.result.error}`);
        }
        state = step.state;
      }
      expect(
        solutionComplete(state, level.solution),
        `checklist incomplete for ${level.id}: ${JSON.stringify(solutionProgress(state, level.solution))}`,
      ).toBe(true);
    }
  });
});
