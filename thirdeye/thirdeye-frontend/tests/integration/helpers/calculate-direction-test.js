import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('helper:calculate-direction', function(hooks) {
  setupRenderingTest(hooks);

  test('when input is positive, calculate direction should return "up"', async function(assert) {
    this.set('inputValue', '1234');
    await render(hbs`{{calculate-direction inputValue}}`);
    assert.equal(
      this.$().text().trim(),
      'up',
      'calculate-direction returns "up" correctly when input is positive');
  });

  test('when input is negative, calculate direction should return "down"', async function(assert) {
    this.set('inputValue', '-1000');
    await render(hbs`{{calculate-direction inputValue}}`);
    assert.equal(this.$().text().trim(),
      'down',
      'calculate-direction returns "down" correctly when input is negative');
  });
});
