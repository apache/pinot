import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('rca-filter-search-bar', 'Integration | Component | rca filter search bar', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{rca-filter-search-bar}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#rca-filter-search-bar}}
      template block text
    {{/rca-filter-search-bar}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
