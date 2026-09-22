import type { LevelProgress } from '../engine/types';
import { allLevels, getNextLevel } from '../levels';

export const STORAGE_KEY = 'learn-airflow-progress-v1';
export const COOKIE_KEY = 'learn_airflow_progress';
const COOKIE_MAX_AGE = 60 * 60 * 24 * 400;

export interface PersistBlob {
  progress: Record<string, LevelProgress>;
  savedAt?: string;
}

export interface CurriculumItem {
  id: string;
  name: string;
  seriesTitle: string;
  commands: string[];
  bestCommands?: number;
}

export interface CurriculumSummary {
  solvedCount: number;
  total: number;
  learned: CurriculumItem[];
  remaining: CurriculumItem[];
  next: CurriculumItem | null;
  percent: number;
}

function readCookie(): string | null {
  if (typeof document === 'undefined') return null;
  const parts = document.cookie.split(';');
  for (const part of parts) {
    const [rawKey, ...rest] = part.trim().split('=');
    if (rawKey !== COOKIE_KEY) continue;
    try {
      return decodeURIComponent(rest.join('='));
    } catch {
      return rest.join('=');
    }
  }
  return null;
}

function writeCookie(payload: string): void {
  if (typeof document === 'undefined') return;
  const encoded = encodeURIComponent(payload);
  document.cookie = `${COOKIE_KEY}=${encoded}; path=/; max-age=${COOKIE_MAX_AGE}; SameSite=Lax`;
}

function parseBlob(raw: string | null): Record<string, LevelProgress> | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as PersistBlob | Record<string, LevelProgress>;
    if (parsed && typeof parsed === 'object' && 'progress' in parsed) {
      return (parsed as PersistBlob).progress ?? {};
    }
    return parsed as Record<string, LevelProgress>;
  } catch {
    return null;
  }
}

export function loadProgress(): Record<string, LevelProgress> {
  let fromLocal: Record<string, LevelProgress> | null = null;
  let fromCookie: Record<string, LevelProgress> | null = null;
  try {
    fromLocal = parseBlob(localStorage.getItem(STORAGE_KEY));
  } catch {
    fromLocal = null;
  }
  try {
    fromCookie = parseBlob(readCookie());
  } catch {
    fromCookie = null;
  }

  const merged: Record<string, LevelProgress> = {};
  for (const src of [fromCookie ?? {}, fromLocal ?? {}]) {
    for (const [id, prog] of Object.entries(src)) {
      if (!prog) continue;
      const prev = merged[id];
      merged[id] = {
        solved: Boolean(prog.solved || prev?.solved),
        bestCommands:
          prev?.bestCommands === undefined
            ? prog.bestCommands
            : prog.bestCommands === undefined
              ? prev.bestCommands
              : Math.min(prev.bestCommands, prog.bestCommands),
      };
    }
  }
  return merged;
}

export function saveProgress(progress: Record<string, LevelProgress>): void {
  const blob: PersistBlob = { progress, savedAt: new Date().toISOString() };
  const payload = JSON.stringify(blob);
  try {
    localStorage.setItem(STORAGE_KEY, payload);
  } catch {
    // quota / private mode
  }
  writeCookie(payload);
}

export function summarizeCurriculum(progress: Record<string, LevelProgress>): CurriculumSummary {
  const learned: CurriculumItem[] = [];
  const remaining: CurriculumItem[] = [];
  let next: CurriculumItem | null = null;

  for (const level of allLevels) {
    const item: CurriculumItem = {
      id: level.id,
      name: level.name,
      seriesTitle: level.seriesTitle,
      commands: level.solution.filter(
        (c) => !/^(airflow dags list|airflow tasks list|airflow tasks states|airflow tasks failed|ls|cat)/.test(c),
      ),
      bestCommands: progress[level.id]?.bestCommands,
    };
    if (progress[level.id]?.solved) learned.push(item);
    else {
      remaining.push(item);
      if (!next) next = item;
    }
  }

  const lastSolvedIdx = allLevels.reduce((acc, l, i) => (progress[l.id]?.solved ? i : acc), -1);
  const officialNext = lastSolvedIdx >= 0 ? getNextLevel(allLevels[lastSolvedIdx]!.id) : allLevels[0];
  if (officialNext && progress[officialNext.id]?.solved) {
    next = remaining[0] ?? null;
  } else if (officialNext) {
    next = remaining.find((r) => r.id === officialNext.id) ?? remaining[0] ?? null;
  }

  return {
    solvedCount: learned.length,
    total: allLevels.length,
    learned,
    remaining,
    next,
    percent: allLevels.length ? Math.round((learned.length / allLevels.length) * 100) : 0,
  };
}

export function resumeLine(summary: CurriculumSummary): string {
  if (!summary.solvedCount) {
    return `No saved progress yet (${summary.total} levels waiting). Start with \`levels\`.`;
  }
  const learnedTitles = summary.learned.map((l) => `${l.name} (${l.id})`).join(' · ');
  const nextText = summary.next
    ? `Next up: ${summary.next.name} — \`${summary.next.commands[0] ?? 'continue'}\``
    : 'All levels cleared.';
  return [
    `Welcome back — progress saved: ${summary.solvedCount}/${summary.total} levels (${summary.percent}%).`,
    `Learned so far: ${learnedTitles}`,
    nextText,
    `Open Levels to resume. Type \`steps\` after starting a level.`,
  ].join('\n');
}
