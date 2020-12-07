import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | stats cards', function(hooks) {
  setupRenderingTest(hooks);

  const CARD = '.te-horizontal-cards__card';

  test('it renders', async function(assert) {
    const fetchedStats = {
      totalAnomalies: {
        value: 10,
        type: 'COUNT'
      },
      responseRate: {
        value: 30,
        type: 'PERCENT'
      },
      precision: {
        value: 40,
        type: 'PERCENT'
      },
      recall: {
        value: 50,
        type: 'PERCENT'
      }
    };
    this.setProperties({ stats: fetchedStats });

    await render(hbs`{{stats-cards
        stats=stats}}`);
    const $title = this.$(`${CARD}-title`);
    const $number = this.$(`${CARD}-number`);

    // Testing titles of all cards
    assert.equal(
      $title.get(0).innerText.trim(),
      'Anomalies',
      'title of 1st card is correct');
    assert.equal(
      $title.get(1).innerText.trim(),
      'Response Rate',
      'title of 2nd card is correct');
    assert.equal(
      $title.get(2).innerText.trim(),
      'Precision',
      'title of 3rd card is correct');
    assert.equal(
      $title.get(3).innerText.trim(),
      'Recall',
      'title of 4th card is correct');  

    // Testing values of all cards
    assert.equal(
      $number.get(0).innerText.trim(),
      10,
      'value of 1st card is correct');
    assert.equal(
      $number.get(1).innerText.trim(),
      '30%',
      'value of 2nd card is correct');
    assert.equal(
      $number.get(2).innerText.trim(),
      '40%',
      'value of 3rd card is correct');
    assert.equal(
      $number.get(3).innerText.trim(),
      '50%',
      'value of 4th card is correct');
  });
});
