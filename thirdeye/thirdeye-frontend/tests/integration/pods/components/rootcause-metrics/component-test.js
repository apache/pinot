import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('rootcause-metrics', 'Integration | Component | rootcause metrics', {
  integration: true
});

test('it renders', function(assert) {
  this.setProperties({
    entities: {},
    aggregates: {},
    scores: {},
    selectedUrns: {},
    onSelection: () => {}
  });

  this.render(hbs`
    {{rootcause-metrics
      entities=entities
      aggregates=aggregates
      scores=scores
      selectedUrns=selectedUrns
      onSelection=(action onSelection)
    }}
  `);

  const table = this.$('.rootcause-metrics');
  assert.ok(table.length, 'It should render properly');
  assert.ok(table.find('.table-header').length, 'It should have headers');
});
