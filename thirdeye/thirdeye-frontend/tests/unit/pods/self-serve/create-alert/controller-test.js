import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Controller | self serve/create alert', function(hooks) {
  setupTest(hooks);

  // Replace this with your real tests.
  test('it exists', function(assert) {
    let controller = this.owner.lookup('controller:self-serve/create-alert');
    assert.ok(controller);
  });
});
