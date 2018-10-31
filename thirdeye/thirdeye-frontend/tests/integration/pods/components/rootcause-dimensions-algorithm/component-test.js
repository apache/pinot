import { module, test, skip } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | rootcause-dimensions-algorithm', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.setProperties({
      metricUrn: 'thirdeye:metric:12345:is_restricted%3Dfalse',
      entities: {
        'thirdeye:metric:12345': {
          label: 'dataset1::metric1'
        }
      },
      context: {
        analysisRange: [ 1525806000000, 1526022000000 ],
        anomalyRange: [ 1525968000000, 1525978800000 ],
        compareMode: 'WoW'
      },
      selectedUrns: {},
      onSelection: () => {}
    });

    await render(hbs`
      {{rootcause-dimensions-algorithm
        entities=entities
        metricUrn=metricUrn
        range=context.analysisRange
        mode=context.compareMode
        selectedUrns=selectedUrns
        isLoading=true
        onSelection=(action onSelection)
      }}
    `);

    const table = this.$('.rootcause-dimensions-table');
    assert.ok(table.length, 'It should render properly');
    assert.ok(table.find('thead tr').length > 0, 'It should have headers');
  });
});
