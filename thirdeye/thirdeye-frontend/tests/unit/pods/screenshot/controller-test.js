import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Controller | screenshot', function(hooks) {
  setupTest(hooks);

  // Replace this with your real tests.
  test('it exists', function(assert) {
    let controller = this.owner.lookup('controller:screenshot');
    assert.ok(controller);
  });
});
