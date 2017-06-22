/**
 * TODO: Test manage/alerts search and filtering
 */
import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('manage/alerts', 'Integration | Component | manage/alerts', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{manage/alerts}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#manage/alerts}}
      template block text
    {{/manage/alerts}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
