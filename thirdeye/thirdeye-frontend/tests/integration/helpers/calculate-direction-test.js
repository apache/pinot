
import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('calculate-direction', 'helper:calculate-direction', {
  integration: true
});

test('when input is positive, calculate direction should return "up"', function(assert) {
  this.set('inputValue', '1234');
  this.render(hbs`{{calculate-direction inputValue}}`);
  assert.equal(
    this.$().text().trim(),
    'up',
    'calculate-direction returns "up" correctly when input is positive');
});

test('when input is negative, calculate direction should return "down"', function(assert) {
  this.set('inputValue', '-1000');
  this.render(hbs`{{calculate-direction inputValue}}`);
  assert.equal(this.$().text().trim(),
    'down',
    'calculate-direction returns "down" correctly when input is negative');
});

