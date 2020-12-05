import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | manage/explore/composite-anomalies', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let route = this.owner.lookup('route:manage/explore/composite-anomalies');
    assert.ok(route);
  });
});
