
import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('calculate-direction', 'helper:calculate-direction', {
  integration: true
});

// Replace this with your real tests.
test('it renders', function(assert) {
  this.set('inputValue', '1234');

  this.render(hbs`{{calculate-direction inputValue}}`);

  assert.equal(this.$().text().trim(), 'up');
});

