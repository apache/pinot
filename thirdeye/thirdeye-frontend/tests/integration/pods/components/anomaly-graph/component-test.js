import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('anomaly-graph', 'Integration | Component | anomaly graph', {
  integration: true
});

test('it renders', function(assert) {
  const selector = '.anomaly-graph';
  this.render(hbs`{{anomaly-graph}}`);

  assert.ok(this.$(selector).length, 'anomaly graph renders correctly');
});
