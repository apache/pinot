import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | rca/details/metrics', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let route = this.owner.lookup('route:rca/details/metrics');
    assert.ok(route);
  });
});
