import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Controller | rca/details/metrics', function(hooks) {
  setupTest(hooks);

  // Replace this with your real tests.
  test('it exists', function(assert) {
    let controller = this.owner.lookup('controller:rca/details/metrics');
    assert.ok(controller);
  });
});