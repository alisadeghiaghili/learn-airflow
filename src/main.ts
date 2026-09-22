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
        'Interactive **Apache Airflow** tutorial in the **learnGitBranching** teaching model.',
        '',
        'Board: **DAG folder → Graph → Runs**. Each level is one idea; solve worlds in order.',
        '',
        `- **Introduction:** init → DAG → task → unpause → trigger (first green run)\n- **Structure:** edges, \`upstream_failed\`, clear/recover\n- **Scheduling:** schedule, catchup, backfill, pause\n- **Ops:** variables, connections, retries, recovery drill`,
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
