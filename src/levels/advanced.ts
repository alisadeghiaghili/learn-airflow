/**
 * Advanced curriculum packs for production-grade Airflow:
 * sensors, deferrable, custom ops, timetables, secrets, deploy, priority, start_date.
 */

import type { AirflowState, LevelDef, TaskDef } from '../engine/types';
import { emptyState, makeDag, makePool, makeTask } from '../engine/state';

type Spec = Partial<TaskDef> & { id: string };

function base(): AirflowState {
  const s = emptyState();
  s.metaInitialized = true;
  s.schedulerRunning = true;
  return s;
}

function dagWith(
  dagId: string,
  opts: {
    schedule?: string | null;
    paused?: boolean;
    catchup?: boolean;
    timetable?: string;
    deployTarget?: string;
    tasks?: Spec[];
    edges?: { from: string; to: string }[];
  },
): AirflowState {
  const s = base();
  const dag = makeDag(dagId, {
    schedule: opts.schedule === undefined ? '@daily' : opts.schedule,
    paused: opts.paused ?? true,
    catchup: opts.catchup ?? false,
    timetable: opts.timetable,
    deployTarget: opts.deployTarget,
    tasks: (opts.tasks ?? []).map((t) => {
      const { id, ...rest } = t;
      return makeTask(id, rest);
    }),
    edges: opts.edges ?? [],
  });
  s.dags = [dag];
  s.activeDagId = dagId;
  return s;
}

export const advancedLevels: LevelDef[] = [
  {
    id: 'adv-1',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'FileSensor + soft_fail',
    difficulty: 4,
    par: 1,
    hint: 'task add ingest wait_for_file --file-sensor --soft-fail',
    objective: 'Add FileSensor `wait_for_file` with soft_fail so a timeout skips instead of failing.',
    goalVisual: 'wait_for_file is FileSensor · soft_fail=True',
    learning: [
      'Sensors poll until a condition is true (or timeout)',
      'soft_fail=True → timeout becomes skipped, not failed',
      'Sensors keep the graph honest: “wait for data” is a node',
    ],
    startDialog: [
      {
        title: 'Waiting is work',
        markdown:
          '```\ntask add ingest wait_for_file --file-sensor --soft-fail\n```\n\n| knob | meaning |\n|------|---------|\n| poke_interval | how often to check |\n| timeout | give up after T |\n| soft_fail | timeout → skipped |\n| mode=deferrable | free the worker while waiting |\n\n**Why not time.sleep in PythonOperator?** It blocks a worker slot for nothing.',
      },
    ],
    startState: dagWith('ingest', { paused: true, tasks: [{ id: 'load' }] }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'sensorConfigured', dagId: 'ingest', taskId: 'wait_for_file', softFail: true },
        { kind: 'taskExists', dagId: 'ingest', taskId: 'wait_for_file' },
      ],
    },
    solution: ['task add ingest wait_for_file --file-sensor --soft-fail'],
  },
  {
    id: 'adv-2',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'Deferrable sensor',
    difficulty: 5,
    par: 1,
    hint: 'task update ingest wait_for_file --deferrable',
    objective: 'Make `wait_for_file` deferrable so it frees the worker slot while waiting.',
    goalVisual: 'wait_for_file deferrable=True',
    learning: [
      'Classic sensors hold a worker slot while poking',
      'Deferrable sensors yield and resume via a triggerer',
      'This is how you scale waits without burning workers',
    ],
    startDialog: [
      {
        title: 'Do not hold the worker',
        markdown:
          '```\ntask update ingest wait_for_file --deferrable\n```\n\nClassic sensor: worker sleeps.\nDeferrable: `execute` yields, a **triggerer** process wakes the task later.\n\nUse when waits can be minutes/hours (upstream SLA, file landings).',
      },
    ],
    startState: dagWith('ingest', {
      paused: true,
      tasks: [{ id: 'wait_for_file', operator: 'FileSensor', sensor: true, softFail: true }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'sensorConfigured', dagId: 'ingest', taskId: 'wait_for_file', deferrable: true }],
    },
    solution: ['task update ingest wait_for_file --deferrable'],
  },
  {
    id: 'adv-3',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'Custom operator',
    difficulty: 4,
    par: 1,
    hint: 'task add etl publish --custom',
    objective: 'Add a CustomOperator task `publish` (BaseOperator subclass).',
    goalVisual: 'publish operator=CustomOperator',
    learning: [
      'Custom operators encapsulate reusable HOW',
      'Hooks own credentials; operators own task semantics',
      'Plugins/providers keep DAGs thin',
    ],
    startDialog: [
      {
        title: 'When built-ins are not enough',
        markdown:
          '```\ntask add etl publish --custom\n```\n\n```python\nclass PublishOperator(BaseOperator):\n    def execute(self, context):\n        hook = WarehouseHook(conn_id=self.conn_id)\n        hook.publish(...)\n```\n\nKeep operators **thin** and hooks **fat**. That is the Airflow extension pattern.',
      },
    ],
    startState: dagWith('etl', { paused: true, tasks: [{ id: 'load' }] }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'customOperator', dagId: 'etl', taskId: 'publish' }],
    },
    solution: ['task add etl publish --custom'],
  },
  {
    id: 'adv-4',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'Custom timetable',
    difficulty: 5,
    par: 1,
    hint: 'dag update report_daily --timetable AfterWorkdayTimetable',
    objective: 'Attach custom timetable `AfterWorkdayTimetable` to `report_daily`.',
    goalVisual: 'timetable=AfterWorkdayTimetable',
    learning: [
      'Cron cannot express “3rd business day” / holiday calendars cleanly',
      'Timetables are first-class schedule objects',
      'Same DAG code, different calendar semantics',
    ],
    startDialog: [
      {
        title: 'Calendars beat cron strings',
        markdown:
          '```\ndag update report_daily --timetable AfterWorkdayTimetable\n```\n\nExamples cron cannot express cleanly:\n\n- after last business day of month\n- first weekday after holiday\n- “run when the trading day closes”\n\n**data_interval** still labels what the run processes.',
      },
    ],
    startState: dagWith('report_daily', {
      schedule: '0 2 * * *',
      paused: true,
      tasks: [{ id: 'build' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'timetableIs', dagId: 'report_daily', timetable: 'AfterWorkdayTimetable' }],
    },
    solution: ['dag update report_daily --timetable AfterWorkdayTimetable'],
  },
  {
    id: 'adv-5',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'start_date hygiene',
    difficulty: 3,
    par: 1,
    hint: 'dag audit start_date',
    objective: 'Audit start_date so it is a fixed calendar time, never `datetime.now()`.',
    goalVisual: 'start_date audit PASS',
    learning: [
      'now() as start_date → schedule drift every parse',
      'Use a fixed datetime (often date(2024,1,1))',
      'Also pin timezone (UTC) explicitly',
    ],
    startDialog: [
      {
        title: 'The start_date footgun',
        markdown:
          '```\ndag audit start_date\n```\n\n**Wrong:** `start_date=datetime.now()` — every parse moves the window.\n**Right:** `start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)`.\n\nThis single mistake causes phantom catchups and “why did it run 40 times?” tickets.',
      },
    ],
    startState: dagWith('report_daily', { paused: true, tasks: [{ id: 'build' }] }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'startDateSafe', dagId: 'report_daily' }],
    },
    solution: ['dag audit start_date'],
  },
  {
    id: 'adv-6',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'Secrets backend',
    difficulty: 3,
    par: 1,
    hint: 'airflow secrets backend set vault',
    objective: 'Switch secrets backend to `vault` (not the metadata DB / env).',
    goalVisual: 'secrets_backend=vault',
    learning: [
      'Variables/Connections in DB are fine for non-secrets',
      'Real credentials belong in Vault / AWS SM / GCP SM',
      'Airflow only needs a backend that can list/get keys',
    ],
    startDialog: [
      {
        title: 'Never commit credentials',
        markdown:
          '```\nairflow secrets backend set vault\n```\n\n| backend | use |\n|---------|-----|\n| airflow | metadata DB (dev) |\n| env | simple local |\n| vault | prod secrets |\n| aws_secrets_manager | AWS prod |\n\nDAG code still says `conn_id="wh"` — only resolution changes.',
      },
    ],
    startState: base(),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'secretsBackendIs', backend: 'vault' }],
    },
    solution: ['airflow secrets backend set vault'],
  },
  {
    id: 'adv-7',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'Deploy target: Kubernetes',
    difficulty: 4,
    par: 1,
    hint: 'airflow deploy set kubernetes',
    objective: 'Mark the active DAG’s deploy target as `kubernetes` and understand the shape.',
    goalVisual: 'deploy target=kubernetes',
    learning: [
      'DAG code stays the same; ops topology changes',
      'K8sExecutor: image + pod template + resources per task',
      'Ship DAGs via image, not a developer laptop mount',
    ],
    startDialog: [
      {
        title: 'From laptop to cluster',
        markdown:
          '```\nairflow deploy set kubernetes\n```\n\nProduction checklist:\n\n1. Docker image with DAGs + deps pinned\n2. KubernetesExecutor or Celery workers\n3. Secrets backend (not env in the image)\n4. Resource requests/limits per task pool\n5. External Postgres metadata DB\n\nSimulator only records the **target** — treat it as a deploy ticket.',
      },
    ],
    startState: dagWith('etl', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'load' }],
      edges: [{ from: 'extract', to: 'load' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'deployTargetIs', dagId: 'etl', target: 'kubernetes' }],
    },
    solution: ['airflow deploy set kubernetes'],
  },
  {
    id: 'adv-8',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'priority_weight',
    difficulty: 3,
    par: 1,
    hint: 'task update etl heavy --priority 10',
    objective: 'Raise `heavy` priority_weight to ≥10 so it wins saturated pool slots.',
    goalVisual: 'heavy priority_weight≥10',
    learning: [
      'Pools limit concurrency; priority_weight orders the queue',
      'SLA-critical tasks should outrank bulk ETL',
      'Does not add resources — only scheduling preference',
    ],
    startDialog: [
      {
        title: 'Who goes first?',
        markdown:
          '```\ntask update etl heavy --priority 10\n```\n\nWhen the pool is full:\n\n| task | priority |\n|------|----------|\n| heavy | 10 |\n| bulk | 1 |\n\n`heavy` gets the next free slot. Classic for “VIP load” vs “hourly dump”.',
      },
    ],
    startState: dagWith('etl', {
      paused: true,
      tasks: [{ id: 'heavy' }, { id: 'bulk' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'priorityAtLeast', dagId: 'etl', taskId: 'heavy', min: 10 }],
    },
    solution: ['task update etl heavy --priority 10'],
  },
  {
    id: 'adv-9',
    series: 'advanced',
    seriesTitle: 'Advanced',
    name: 'Production capstone',
    difficulty: 5,
    par: 7,
    hint: 'pool, priority, secrets, start_date, deferrable sensor, deploy, trigger rule',
    objective:
      'Hardening pass: pool `gold` (1 slot), priority 10 on `load`, secrets=vault, start_date audit, deferrable sensor, deploy=kubernetes, cleanup trigger_rule=all_done.',
    goalVisual: 'pool gold · priority 10 · vault · start_date safe · deferrable · k8s · all_done',
    learning: [
      'This is the “day 2” Airflow skill: not just green graphs',
      'Concurrency + secrets + deploy + failure semantics together',
    ],
    startDialog: [
      {
        title: 'Day-2 checklist',
        markdown:
          '```\npool set gold 1\ntask update prod load --pool gold --priority 10\nairflow secrets backend set vault\ndag audit start_date\ntask update prod wait_for_file --deferrable\nairflow deploy set kubernetes\ntask update prod cleanup --trigger-rule all_done\n```\n\nIf you can do this drill without the hint, you are dangerous in a real Airflow cluster.',
      },
    ],
    startState: dagWith('prod', {
      paused: true,
      schedule: '0 2 * * *',
      tasks: [
        { id: 'wait_for_file', operator: 'FileSensor', sensor: true, softFail: true },
        { id: 'load' },
        { id: 'cleanup', triggerRule: 'all_success' as const },
      ],
      edges: [
        { from: 'wait_for_file', to: 'load' },
        { from: 'load', to: 'cleanup' },
      ],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'poolExists', name: 'gold', slotsAtLeast: 1 },
        { kind: 'taskPoolIs', dagId: 'prod', taskId: 'load', pool: 'gold' },
        { kind: 'priorityAtLeast', dagId: 'prod', taskId: 'load', min: 10 },
        { kind: 'secretsBackendIs', backend: 'vault' },
        { kind: 'startDateSafe', dagId: 'prod' },
        { kind: 'sensorConfigured', dagId: 'prod', taskId: 'wait_for_file', deferrable: true },
        { kind: 'deployTargetIs', dagId: 'prod', target: 'kubernetes' },
        { kind: 'triggerRuleIs', dagId: 'prod', taskId: 'cleanup', rule: 'all_done' },
      ],
    },
    solution: [
      'pool set gold 1',
      'task update prod load --pool gold --priority 10',
      'airflow secrets backend set vault',
      'dag audit start_date',
      'task update prod wait_for_file --deferrable',
      'airflow deploy set kubernetes',
      'task update prod cleanup --trigger-rule all_done',
    ],
  },
];

// silence unused if tree-shaken
void makePool;
