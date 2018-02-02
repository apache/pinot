import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

moduleForAcceptance('Acceptance | edit alert');

const METRIC_NAME = `#select-metric`;
const GRANULARITY = `#select-granularity`;
const ALERT_NAME = `#anomaly-form-function-name`;
const STATUS = '.te-toggle--form span';
const EDIT_LINK = '/manage/alerts/edit';
const STATUS_TOGGLER = '.x-toggle-btn';
const SUBMIT_BUTTON = '.te-button--submit';

test(`visiting ${EDIT_LINK} and checking that fields render correctly and edit is successful`, async (assert) => {
  const alert = server.create('alert');
  await visit(`/manage/alerts/${alert.id}`);

  assert.equal(
    currentURL(),
    '/manage/alerts/1',
    'correctly redirects to edit alerts page'
  );
  assert.equal(
    find(METRIC_NAME).get(0).value,
    'test_metric_1',
    'metric name is correct');
  assert.equal(
    find(GRANULARITY).get(0).value,
    '1_DAYS',
    'granularity is correct');
  assert.equal(
    find(ALERT_NAME).get(0).value,
    'test_function_1',
    'alert name is correct');
  assert.equal(
    find(STATUS).get(0).innerText,
    'Active',
    'alert status is correct');

  await fillIn(ALERT_NAME, 'test_function_2');
  await click(STATUS_TOGGLER);
  await click(SUBMIT_BUTTON);

  assert.equal(
    currentURL(),
    '/manage/alerts',
    'correctly redirects to manage alerts page after edit'
  );

  await visit(`/manage/alerts/${alert.id}`);

  assert.equal(
    find(ALERT_NAME).get(0).value,
    'test_function_2',
    'after edit, alert name is saved correctly');
  assert.equal(
    find(STATUS).get(0).innerText,
    'Inactive',
    'after edit, alert status is saved correctly');
});
