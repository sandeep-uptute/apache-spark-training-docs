// Offline authoring-host validation. The Ubuntu learner check is the Python version.
const fs = require('fs');
const path = require('path');
const root = path.resolve(__dirname, '..');
const required = [
  'START_HERE.md', 'DELIVERY_GUIDE.md', 'LEARNER_HANDBOOK.md', 'PRESENTATION.md',
  'PROJECT_GUIDE.md', 'SETUP_AND_TROUBLESHOOTING.md', 'QUICK_REFERENCE.md',
  'KNOWLEDGE_CHECK.md', 'exercises/TOPIC_EXERCISES.md',
  'solutions/EXERCISE_SOLUTIONS.md', 'solutions/KNOWLEDGE_CHECK_ANSWERS.md',
  'solutions/project_expected_metrics.json', 'project/pipeline.py', 'project/run_config.json',
  'project-notebooks/07_guided_project_starter.ipynb',
  'project-notebooks/08_project_reference_solution.ipynb',
  'project/source/run-001/transactions.csv', 'project/source/run-002/transactions.json',
  'project/source/run-003/transactions.tsv', 'project/reference/customers.csv',
  'project/reference/products.json'
];
const missing = required.filter(f => !fs.existsSync(path.join(root, f)));
if (missing.length) throw new Error(`Missing files: ${missing.join(', ')}`);
for (const file of ['project/run_config.json', 'solutions/project_expected_metrics.json',
  'project-notebooks/07_guided_project_starter.ipynb',
  'project-notebooks/08_project_reference_solution.ipynb']) {
  JSON.parse(fs.readFileSync(path.join(root, file), 'utf8'));
}
const lines = f => fs.readFileSync(path.join(root, f), 'utf8').trim().split(/\r?\n/).length;
if (lines('project/source/run-001/transactions.csv') - 1 !== 12) throw new Error('run-001 row count');
if (lines('project/source/run-002/transactions.json') !== 10) throw new Error('run-002 row count');
if (lines('project/source/run-003/transactions.tsv') - 1 !== 9) throw new Error('run-003 row count');
console.log('FINAL PACKAGE STRUCTURE PASS');

