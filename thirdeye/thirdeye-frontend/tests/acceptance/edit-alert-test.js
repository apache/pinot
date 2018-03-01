import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

moduleForAcceptance('Acceptance | edit alert');

const ALERT_NAME_INPUT = '#anomaly-form-function-name';
const SUBSCRIPTION_GROUP = '#anomaly-form-app-name';
const STATUS = '.te-toggle--form span';
const STATUS_RESULT = '.te-search-results__tag';
const EDIT_LINK = '/manage/alert/edit';
const STATUS_TOGGLER = '.x-toggle-btn';
const SUBMIT_BUTTON = '.te-button--submit';
const NEW_FUNC_NAME = 'test_function_2';
const NEW_FUNC_RESULT = '.te-search-results__title-name';

test(`visiting ${EDIT_LINK} and checking that fields render correctly and edit is successful`, async (assert) => {
  const alert = server.create('alert');
  await visit(`/manage/alert/${alert.id}/edit`);

  assert.equal(
    currentURL(),
    '/manage/alert/1/edit',
    'correctly redirects to edit alerts page'
  );
  assert.equal(
    find(ALERT_NAME_INPUT).get(0).value,
    'test_function_1',
    'alert name is correct');
  assert.equal(
    find(SUBSCRIPTION_GROUP).get(0).innerText,
    'beauty-and-the-beast',
    'subscription group name is correct');
  assert.equal(
    find(STATUS).get(0).innerText,
    'Active',
    'alert status is correct');

  await fillIn(ALERT_NAME_INPUT, NEW_FUNC_NAME);
  await click(STATUS_TOGGLER);
  await click(SUBMIT_BUTTON);

  assert.equal(
    currentURL(),
    '/manage/alerts',
    'correctly redirects to manage alerts page after edit'
  );

  assert.equal(
    find(`${NEW_FUNC_RESULT}[title='${NEW_FUNC_NAME}']`).get(0).innerText,
    NEW_FUNC_NAME,
    'after edit, alert name is saved correctly');
  assert.equal(
    find(STATUS_RESULT).get(0).innerText,
    'Inactive',
    'after edit, alert status is saved correctly');
});
