import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

moduleForAcceptance('Acceptance | edit alert');

const EDIT_BUTTON = '.te-search-results__edit-button';

test('visiting /edit-alert', function(assert) {
  visit('/manage/alerts');

  andThen(function() {
    assert.equal(
      currentURL(),
      '/manage/alerts',
      'manage alerts url is correct');
  });
});
