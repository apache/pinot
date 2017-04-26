import { moduleFor, test } from 'ember-qunit';

moduleFor('route:example', 'Unit | Route | example', {
  // Specify the other units that are required for this test.
  needs: ['service:redux']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
