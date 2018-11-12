import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import wait from 'ember-test-helpers/wait';

module('Integration | Component | rootcause-anomaly', function(hooks) {
  setupRenderingTest(hooks);

  test(`displays warning if anomaly function value and displayed value are more than 1
    percent different`, async function(assert) {
    this.setProperties({
      entities: {
        'thirdeye:event:anomaly:1': {
          urn: 'thirdeye:event:anomaly:1',
          label: 'dataset1::metric1',
          attributes : {
            EXTERNAL : [ 'hello' ],
            weight : [ '0.17679628053981644' ],
            baseline : [ '3308.878952874078' ],
            externalUrls : [ 'EXTERNAL' ],
            statusClassification : [ 'NONE' ],
            score : [ '0.03195732831954956' ],
            functionId : [ '1' ],
            current : [ '3' ],
            aggregateMultiplier : [ '0.041666666666666664' ],
            metricId : [ '1' ],
            metric : [ 'metric' ],
            function : [ 'function' ],
            comment : [ '' ],
            metricGranularity : [ '5_MINUTES' ],
            dataset : [ 'my_dataset' ],
            status : [ 'NO_FEEDBACK' ]
          }
        }
      },
      anomalyUrns: ['thirdeye:event:anomaly:1', 'thirdeye:metric:1'],
      onFeedback: () => {},
      aggregates: {
        'frontend:metric:current:1': 93453.15844726562
      }
    });

    await render(hbs`
      {{rootcause-anomaly
        entities=entities
        aggregates=aggregates
        anomalyUrns=anomalyUrns
        onFeedback=(action onFeedback)
      }}
    `);

    //await wait();
    assert.ok(this.$('.diffcurrent-alert').length > 0);
  });

  test(`does not display warning if anomaly function value and displayed value are not more than 1
    percent different`, async function(assert) {
      this.setProperties({
        entities: {
          'thirdeye:event:anomaly:1': {
            urn: 'thirdeye:event:anomaly:1',
            label: 'dataset1::metric1',
            attributes : {
              EXTERNAL : [ 'hello' ],
              weight : [ '0.17679628053981644' ],
              baseline : [ '3308.878952874078' ],
              externalUrls : [ 'EXTERNAL' ],
              statusClassification : [ 'NONE' ],
              score : [ '0.03195732831954956' ],
              functionId : [ '1' ],
              current : [ '3893.881601969401' ],
              aggregateMultiplier : [ '0.041666666666666664' ],
              metricId : [ '1' ],
              metric : [ 'metric' ],
              function : [ 'function' ],
              comment : [ '' ],
              metricGranularity : [ '5_MINUTES' ],
              dataset : [ 'my_dataset' ],
              status : [ 'NO_FEEDBACK' ]
            }
          }
        },
        anomalyUrns: ['thirdeye:event:anomaly:1', 'thirdeye:metric:1'],
        onFeedback: () => {},
        aggregates: {
          'frontend:metric:current:1': 93453.15844726562
        }
      });

    await render(hbs`
      {{rootcause-anomaly
        entities=entities
        aggregates=aggregates
        anomalyUrns=anomalyUrns
        onFeedback=(action onFeedback)
      }}
    `);

    assert.notOk(this.$('.diffcurrent-alert').length > 0);
  });
});
