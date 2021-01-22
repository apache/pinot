import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('composite-anomalies', 'Integration | Component | composite anomalies', {
  integration: true
});

test('it renders', function (assert) {
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.setProperties({
    alertId: 123,
    anomalies: []
  });

  this.render(hbs`{{composite-anomalies alertId=alertId anomalies=anomalies}}`);

  assert.ok(this.$().text().trim().includes('Alert Anomalies'));
});
