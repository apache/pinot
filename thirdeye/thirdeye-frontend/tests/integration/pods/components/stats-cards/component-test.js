import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('stats-cards', 'Integration | Component | stats cards', {
  integration: true
});

const CARD = '.te-horizontal-cards__card';
const TITLE = `${CARD}-title`;
const DESCRIPTION = `${CARD}-text`;
const NUMBER = `${CARD}-number`;

test('it renders', function(assert) {
  const stats = [
    ['Number of anomalies', 'total anomalies', 10],
    ['Response Rate', 'description of response', 0.9],
    ['Precision', 'description of precision', 1]
  ];
  this.setProperties({ stats });

  this.render(
    hbs`{{stats-cards
          stats=stats}}`
  );

  // Testing titles of all cards
  assert.equal(
    this.$(TITLE).get(0).innerText,
    stats[0][0],
    'title of 1st card is correct');
  assert.equal(
    this.$(TITLE).get(1).innerText,
    stats[1][0],
    'title of 2nd card is correct');
  assert.equal(
    this.$(TITLE).get(2).innerText,
    stats[2][0],
    'title of 3rd card is correct');

  // Testing descriptions of all cards
  assert.equal(
    this.$(DESCRIPTION).get(0).innerText,
    stats[0][1],
    'description of 1st card is correct');
  assert.equal(
    this.$(DESCRIPTION).get(1).innerText,
    stats[1][1],
    'description of 2nd card is correct');
  assert.equal(
    this.$(DESCRIPTION).get(2).innerText,
    stats[2][1],
    'description of 3rd card is correct');

  // Testing values of all cards
  assert.equal(
    this.$(NUMBER).get(0).innerText,
    stats[0][2],
    'value of 1st card is correct');
  assert.equal(
    this.$(NUMBER).get(1).innerText,
    stats[1][2],
    'value of 2nd card is correct');
  assert.equal(
    this.$(NUMBER).get(2).innerText,
    stats[2][2],
    'value of 3rd card is correct');
});
