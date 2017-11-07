import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import events from 'thirdeye-frontend/mocks/sampleEvents';
import filterBlocks from 'thirdeye-frontend/mocks/filterBarConfig';

const FILTER_BAR_TITLE = '.entity-filter__group-title';

moduleForComponent('rca-poc-table', 'Integration | Component | rca poc table', {
  integration: true
});

test('it renders', function(assert) {
  this.setProperties({
    events,
    filterBlocks
  });

  this.render(hbs`{{rca-poc-table
                    events=events
                    filterBlocks=filterBlocks}}`);
  assert.equal(
    this.$(FILTER_BAR_TITLE).get(0).innerText.trim(),
    'Holiday',
    'renders correct holiday filter');
  assert.equal(
    this.$(FILTER_BAR_TITLE).get(1).innerText.trim(),
    'Deployment',
    'renders correct holiday filter');
  assert.equal(
    this.$(FILTER_BAR_TITLE).get(2).innerText.trim(),
    'Other Anomalies',
    'renders correct holiday filterq');
});
