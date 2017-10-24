import { moduleFor, test } from 'ember-qunit';

moduleFor('route:rca/details/metrics', 'Unit | Route | rca/details/metrics', {
  // Specify the other units that are required for this test.
  needs: ['service:redux']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
