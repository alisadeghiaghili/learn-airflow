/**
 * Deterministic fake ids for the Airflow simulator.
 * Not cryptographic — stable ids so levels and goals stay comparable.
 */

export function fakeId(seed: string): string {
  let h1 = 0x811c9dc5;
  let h2 = 0x01000193;
  for (let i = 0; i < seed.length; i++) {
    const c = seed.charCodeAt(i);
    h1 ^= c;
    h1 = Math.imul(h1, 0x01000193) >>> 0;
    h2 = (h2 + c * (i + 3)) >>> 0;
  }
  const part = (n: number) => n.toString(16).padStart(8, '0');
  return part(h1) + part(h2);
}

export function shortId(id: string): string {
  return id.slice(0, 8);
}

export function manualRunId(logicalDate: string): string {
  return `manual__${logicalDate}`;
}

export function scheduledRunId(logicalDate: string): string {
  return `scheduled__${logicalDate}`;
}

export function backfillRunId(logicalDate: string): string {
  return `backfill__${logicalDate}`;
}
