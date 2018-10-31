import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | stats cards', function(hooks) {
  setupRenderingTest(hooks);

  const CARD = '.te-horizontal-cards__card';

  test('it renders', async function(assert) {
    const stats = [
      ['Number of anomalies', 'total anomalies', 10],
      ['Response Rate', 'description of response', 0.9],
      ['Precision', 'description of precision', 1]
    ];
    this.setProperties({ stats });

    await render(hbs`{{stats-cards
        stats=stats}}`);
    const $title = this.$(`${CARD}-title`);
    const $description = this.$(`${CARD}-text`);
    const $number = this.$(`${CARD}-number`);

    // Testing titles of all cards
    assert.equal(
      $title.get(0).innerText.trim(),
      stats[0][0],
      'title of 1st card is correct');
    assert.equal(
      $title.get(1).innerText.trim(),
      stats[1][0],
      'title of 2nd card is correct');
    assert.equal(
      $title.get(2).innerText.trim(),
      stats[2][0],
      'title of 3rd card is correct');

    // Testing descriptions of all cards
    assert.equal(
      $description.get(0).innerText.trim(),
      stats[0][1],
      'description of 1st card is correct');
    assert.equal(
      $description.get(1).innerText.trim(),
      stats[1][1],
      'description of 2nd card is correct');
    assert.equal(
      $description.get(2).innerText.trim(),
      stats[2][1],
      'description of 3rd card is correct');

    // Testing values of all cards
    assert.equal(
      $number.get(0).innerText.trim(),
      stats[0][2],
      'value of 1st card is correct');
    assert.equal(
      $number.get(1).innerText.trim(),
      stats[1][2],
      'value of 2nd card is correct');
    assert.equal(
      $number.get(2).innerText.trim(),
      stats[2][2],
      'value of 3rd card is correct');
  });
});
