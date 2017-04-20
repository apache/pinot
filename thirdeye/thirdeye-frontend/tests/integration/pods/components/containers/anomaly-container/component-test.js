import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('containers/anomaly-container', 'Integration | Component | containers/anomaly container', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{containers/anomaly-container}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#containers/anomaly-container}}
      template block text
    {{/containers/anomaly-container}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
