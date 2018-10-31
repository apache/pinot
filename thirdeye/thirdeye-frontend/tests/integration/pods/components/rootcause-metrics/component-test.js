import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | rootcause metrics', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.setProperties({
      entities: {},
      aggregates: {},
      scores: {},
      selectedUrns: {},
      onSelection: () => {}
    });

    await render(hbs`
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
});
