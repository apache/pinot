import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | home/share-dashboard', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let route = this.owner.lookup('route:home/share-dashboard');
    assert.ok(route);
  });
});
