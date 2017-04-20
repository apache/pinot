import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('anomaly-graph', 'Integration | Component | anomaly graph', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{anomaly-graph}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#anomaly-graph}}
      template block text
    {{/anomaly-graph}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
