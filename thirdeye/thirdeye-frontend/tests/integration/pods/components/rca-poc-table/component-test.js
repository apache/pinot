import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('rca-poc-table', 'Integration | Component | rca poc table', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{rca-poc-table}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#rca-poc-table}}
      template block text
    {{/rca-poc-table}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
