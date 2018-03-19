import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | self serve/create alert', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let route = this.owner.lookup('route:self-serve/create-alert');
    assert.ok(route);
  });
});
