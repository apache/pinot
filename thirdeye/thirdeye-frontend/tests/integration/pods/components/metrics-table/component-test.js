import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('metrics-table', 'Integration | Component | metrics table', {
  integration: true
});

test('it renders', function(assert) {
  this.setProperties({
    urns: [],
    entities: {},
    links: {},
    selectedUrns: {},
    toggleSelection: () => {}
  });

  this.render(hbs`
    {{metrics-table
      urns=urns
      entities=entities
      links=links
      selectedUrns=selectedUrns
      toggleSelection=(action toggleSelection)
    }}
  `);

  const table = this.$('.metrics-table');
  assert.ok(table.length, 'It should render properly');
  assert.ok(table.find('.table-header').length, 'It should have headers');
});
