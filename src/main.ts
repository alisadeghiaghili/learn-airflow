import './style.css';
import { App } from './ui/app';
import { allLevels } from './levels';
import { showModal, renderMarkdown } from './ui/dialog';

const root = document.querySelector('#app');
if (!root) throw new Error('#app root missing');

const app = new App(root as HTMLElement);

const params = new URLSearchParams(window.location.search);
if (!params.has('NODEMO')) {
  showModal({
    title: 'LearnAirflow',
    bodyHtml: renderMarkdown(
      [
        'Interactive **Apache Airflow** tutorial — sandbox + guided levels.',
        '',
        'Board: **DAG folder → Graph → Runs**. Each level is one idea; solve worlds in order.',
        '',
        `- **Introduction:** architecture, DAG, task, logical date, trigger, dag.test()\n- **Structure:** edges, trigger rules, branching, clear/recover\n- **Data & TaskFlow:** XCom, @task, templates, dynamic mapping\n- **Scheduling:** cron, max_active_runs, catchup/backfill, datasets\n- **Ops:** retries, pools/SLA, connections, executor, logs, capstone`,
        '',
        'Meta: `levels`, `hint`, `show goal`, `show solution`, `reset`, `undo`, `sandbox`, `help`.',
        '',
        `**${allLevels.length}** levels. Open Levels and start at \`intro-1\`.`,
      ].join('\n'),
    ),
    actions: [
      {
        label: 'Sandbox',
        className: 'ghost',
        onClick: () => undefined,
      },
      {
        label: 'Open levels',
        className: 'primary',
        onClick: () => {
          const btn = document.querySelector<HTMLButtonElement>('[data-action="levels"]');
          btn?.click();
        },
      },
    ],
  });
}

(window as unknown as { learnAirflowApp: App }).learnAirflowApp = app;
