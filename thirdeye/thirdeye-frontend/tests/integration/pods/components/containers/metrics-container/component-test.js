import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('containers/metrics-container', 'Integration | Component | containers/metrics container', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{containers/metrics-container}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#containers/metrics-container}}
      template block text
    {{/containers/metrics-container}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
