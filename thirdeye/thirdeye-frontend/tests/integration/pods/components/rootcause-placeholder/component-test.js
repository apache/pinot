import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('rootcause-placeholder', 'Integration | Component | rootcause placeholder', {
  integration: true
});

test('it renders', function(assert) {
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{rootcause-placeholder}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#rootcause-placeholder}}
      template block text
    {{/rootcause-placeholder}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
