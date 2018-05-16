import { module, test, skip } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | rootcause-dimensions', function(hooks) {
  setupRenderingTest(hooks);

  skip('it renders', async function(assert) {
    this.setProperties({
      metricUrn: 'thirdeye:metric:12345',
      entities: {
        'thirdeye:metric:12345': {
          label: 'thirdeyeKbmi::warmSignUps'
        }
      },
      context: {
        analysisRange: [ 1525806000000, 1526022000000 ],
        anomalyRange: [ 1525968000000, 1525978800000 ],
        compareMode: 'WoW'
      }
    });

    await render(hbs`
      {{rootcause-dimensions
        selectedUrn=metricUrn
        entities=entities
        context=context
      }}
    `);

    const table = this.$('.rootcause-dimensions-table');
    assert.ok(table.length, 'It should render properly');
    assert.ok(table.find('.table-header').length, 'It should have headers');
  });
});
