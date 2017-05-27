import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';
import { faker } from 'ember-cli-mirage';

moduleForAcceptance('Acceptance | example');

test('visiting /example', function(assert) {
  const id = faker.random.number();

  visit(`/example/${id}`);

  andThen(() => {
    assert.equal(currentURL(), `/example/${id}`);
    assert.ok(find('.anomaly-graph').length, 'should see the graph component');
  });
});
