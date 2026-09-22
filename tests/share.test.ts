import { describe, expect, it } from 'vitest';
import { buildShareTargets, shareMessageLinkedIn, shareMessageX } from '../src/ui/share';
import { summarizeCurriculum } from '../src/ui/progress';
import { allLevels } from '../src/levels';

function fakeProgress(ids: string[]): Record<string, { solved: boolean; bestCommands?: number }> {
  const p: Record<string, { solved: boolean; bestCommands?: number }> = {};
  for (const id of ids) p[id] = { solved: true, bestCommands: 3 };
  return p;
}

describe('curriculum share messages', () => {
  it('lists learned levels in the LinkedIn post', () => {
    const curriculum = summarizeCurriculum(fakeProgress(['intro-1', 'intro-2']));
    const text = shareMessageLinkedIn({
      levelName: 'Author a DAG',
      levelId: 'intro-2',
      commands: 2,
      par: 2,
      curriculum,
    });
    expect(text).toContain("I'm learning Apache Airflow");
    expect(curriculum.solvedCount).toBe(2);
    expect(curriculum.learned).toHaveLength(2);
    expect(text).toContain('Introduction: Initialize Airflow');
    expect(text).toContain('Introduction: Author a DAG');
    expect(text).toContain('learn-airflow');
  });

  it('X text stays short and mentions progress', () => {
    const ids = allLevels.map((l) => l.id);
    const curriculum = summarizeCurriculum(fakeProgress(ids.slice(0, 5)));
    const short = shareMessageX({
      levelName: 'x',
      levelId: 'intro-3',
      commands: 2,
      par: 2,
      curriculum,
    });
    expect(short).toContain('5/');
    expect(short.length).toBeLessThanOrEqual(280);
  });

  it('share targets include curriculum in LinkedIn text param', () => {
    const curriculum = summarizeCurriculum(fakeProgress(['intro-1']));
    const targets = buildShareTargets({
      levelName: 'Initialize Airflow',
      levelId: 'intro-1',
      commands: 1,
      par: 1,
      curriculum,
    });
    expect(targets.linkedin).toContain('linkedin.com');
    expect(decodeURIComponent(targets.linkedin)).toContain('Initialize Airflow');
  });
});
