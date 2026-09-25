/**
 * Multi-step production incident drill (CLI-only transfer test).
 */

import type { AirflowState, LevelDef } from '../engine/types';
import { createRun, emptyState, findDag, makeDag, makeTask, processRun, cloneState } from '../engine/state';

function incidentState(): AirflowState {
  const s = emptyState();
  s.metaInitialized = true;
  s.schedulerRunning = true;
  s.executor = 'CeleryExecutor';
  s.secretsBackend = 'vault';
  s.pools = [{ name: 'gold', slots: 1, used: 0 }];
  const dag = makeDag('prod_billing', {
    schedule: '0 2 * * *',
    paused: false,
    catchup: false,
    maxActiveRuns: 1,
    startDate: '2024-01-01T00:00:00+00:00',
    tasks: [
      makeTask('wait_file', { operator: 'FileSensor', sensor: true, softFail: true, deferrable: true }),
      makeTask('extract', { operator: 'PythonOperator', retries: 1 }),
      makeTask('charge', { operator: 'PythonOperator', retries: 0, failAttempts: 1, pool: 'gold', priorityWeight: 5, slaMinutes: 60, emailOnFailure: true }),
      makeTask('cleanup', { operator: 'PythonOperator', triggerRule: 'all_done' }),
    ],
    edges: [
      { from: 'wait_file', to: 'extract' },
      { from: 'extract', to: 'charge' },
      { from: 'charge', to: 'cleanup' },
    ],
    usesTaskFlow: false,
  });
  // failed overnight run
  const run = createRun(dag, '2024-06-01T02:00:00+00:00', 'scheduled');
  dag.runs.push(run);
  processRun(s, dag, run);
  s.dags = [dag];
  s.activeDagId = 'prod_billing';
  s.startDateSafe = true;
  return s;
}

export const incidentLevels: LevelDef[] = [
  {
    id: 'inc-1',
    series: 'incident',
    seriesTitle: 'Incident',
    name: 'Triage: what broke?',
    difficulty: 4,
    par: 2,
    hint: 'logs show prod_billing charge; airflow tasks failed prod_billing',
    objective:
      'On-call drill step 1: open logs for `charge` and list failed tasks. Leave evidence of triage (logs show).',
    goalVisual: 'logExists charge · failed tasks inspected',
    learning: [
      'Start from task state + logs, not from random retrigger',
      'try_number tells you which attempt failed',
      'SLA is already ticking — triage before you code',
    ],
    startDialog: [
      {
        title: '02:14 — pager',
        markdown:
          'Overnight `prod_billing` is red. You have 15 minutes before the business call.\n\n```\nairflow tasks failed prod_billing\nlogs show prod_billing charge\nairflow tasks states-for-dag-run prod_billing scheduled__\n```\n\n**Do not** clear yet. First answer: *which task, which try, what does the traceback say?*',
      },
    ],
    startState: incidentState(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'logExists', dagId: 'prod_billing', taskId: 'charge' },
        { kind: 'taskState', dagId: 'prod_billing', taskId: 'charge', state: 'failed' },
      ],
    },
    solution: ['logs show prod_billing charge'],
  },
  {
    id: 'inc-2',
    series: 'incident',
    seriesTitle: 'Incident',
    name: 'Containment: clear + recover',
    difficulty: 5,
    par: 2,
    hint: 'airflow tasks clear prod_billing -t charge -y',
    objective:
      'Step 2: after confirming the failure is transient/config, clear `charge` so the run can finish (cleanup runs via all_done).',
    goalVisual: 'charge success · cleanup ran (all_done) · run green',
    learning: [
      'clear the failing TI, not the whole history',
      'trigger_rule=all_done lets cleanup run after failure OR success',
      'Containment first, root-cause PR after',
    ],
    startDialog: [
      {
        title: '02:31 — contain',
        markdown:
          '```\nairflow tasks clear prod_billing -t charge -y\n```\n\nWatch the SVG: `charge` retried → `cleanup` (all_done) can proceed.\n\n**Why all_done?** Money jobs need a reconciliation step whether or not charge succeeded.',
      },
    ],
    startState: (() => {
      const s = incidentState();
      // after triage logs exist — still failed
      return s;
    })(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'taskState', dagId: 'prod_billing', taskId: 'charge', state: 'success' },
        { kind: 'taskState', dagId: 'prod_billing', taskId: 'cleanup', state: 'success' },
        { kind: 'runSuccess', dagId: 'prod_billing' },
      ],
    },
    solution: ['airflow tasks clear prod_billing -t charge -y'],
  },
  {
    id: 'inc-3',
    series: 'incident',
    seriesTitle: 'Incident',
    name: 'Harden: retries + SLA',
    difficulty: 4,
    par: 2,
    hint: 'task update prod_billing charge --retries 2 --email-on-failure',
    objective:
      'Step 3: raise `charge` retries to ≥2 and enable email_on_failure so the next incident notifies.',
    goalVisual: 'charge retries≥2 · email_on_failure=True',
    learning: [
      'Incidents must change config, not only state',
      'email_on_failure is the first signal path',
      'Retries absorb transients without a pager',
    ],
    startDialog: [
      {
        title: '02:55 — prevent the next page',
        markdown:
          '```\ntask update prod_billing charge --retries 2 --email-on-failure\n```\n\nIf this is all you do, the same bug pages you next week. Pair with a code fix + tests.',
      },
    ],
    startState: incidentState(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'retriesAtLeast', dagId: 'prod_billing', taskId: 'charge', min: 2 },
        { kind: 'emailOnFailure', dagId: 'prod_billing', taskId: 'charge', value: true },
      ],
    },
    solution: [
      'task update prod_billing charge --retries 2 --email-on-failure',
    ],
  },
  {
    id: 'inc-4',
    series: 'incident',
    seriesTitle: 'Incident',
    name: 'Close the ticket',
    difficulty: 5,
    par: 5,
    hint: 'run inc-1..3 skills: logs, clear, harden, start_date audit, deploy note',
    objective:
      'Final step: recover the run to success AND harden (retries+email+priority) AND audit start_date AND set deploy target kubernetes.',
    goalVisual: 'run success · retries≥2 · email · priority≥10 · start_date safe · kubernetes',
    learning: [
      'A closed incident = green board + changed config + deploy note',
      'This is the full on-call loop, not just “make it green”',
    ],
    startDialog: [
      {
        title: '03:20 — closeout checklist',
        markdown:
          '```\nairflow tasks clear prod_billing -t charge -y\ntask update prod_billing charge --retries 2 --email-on-failure --priority 10\ndag audit start_date\nairflow deploy set kubernetes\n```\n\nTicket template: *impact · root cause · fix · prevention*. If you cannot write that, you are not done.',
      },
    ],
    startState: incidentState(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'runSuccess', dagId: 'prod_billing' },
        { kind: 'retriesAtLeast', dagId: 'prod_billing', taskId: 'charge', min: 2 },
        { kind: 'emailOnFailure', dagId: 'prod_billing', taskId: 'charge', value: true },
        { kind: 'priorityAtLeast', dagId: 'prod_billing', taskId: 'charge', min: 10 },
        { kind: 'startDateSafe', dagId: 'prod_billing' },
        { kind: 'deployTargetIs', dagId: 'prod_billing', target: 'kubernetes' },
      ],
    },
    solution: [
      'airflow tasks clear prod_billing -t charge -y',
      'task update prod_billing charge --retries 2 --email-on-failure --priority 10',
      'dag audit start_date',
      'airflow deploy set kubernetes',
    ],
  },
];

void cloneState;
void findDag;
