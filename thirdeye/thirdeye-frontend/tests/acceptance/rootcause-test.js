import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

moduleForAcceptance('Acceptance | rootcause');

test('visiting /rootcause', function(assert) {
  visit('/rootcause');

  andThen(function() {
    assert.equal(currentURL(), '/rootcause');
  });
});
