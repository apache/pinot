import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

moduleForAcceptance('Acceptance | edit alert');

const EDIT_BUTTON = '.te-search-results__edit-button a';
const METRIC_NAME = `#select-metric`;
const GRANULARITY = `#select-granularity`;
const ALERT_NAME = `#anomaly-form-function-name`;
const STATUS = '.te-toggle--form span';

test('visiting /manage/alerts/edit and checking that fields render correctly', assert => {
  visit('/manage/alerts');

  andThen(() => {
    assert.equal(
      currentURL(),
      '/manage/alerts',
      'manage alerts url is correct');
    click(find(EDIT_BUTTON).get(1));
  });

  andThen(() => {
    assert.equal(
      currentURL(),
      '/manage/alerts/10',
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
  });
});
