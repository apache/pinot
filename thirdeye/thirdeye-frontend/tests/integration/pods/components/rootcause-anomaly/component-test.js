import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import wait from 'ember-test-helpers/wait';

module('Integration | Component | rootcause-anomaly', function(hooks) {
  setupRenderingTest(hooks);

  test(`displays warning if anomaly function value and displayed value are more
    than 1 percent different`, async function(assert) {
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
            metricGranularity : [ '1_DAYS' ],
            dataset : [ 'my_dataset' ],
            status : [ 'NO_FEEDBACK' ]
          },
          start: 0,
          end: 0
        }
      },
      anomalyUrns: ['thirdeye:event:anomaly:1', 'thirdeye:metric:1'],
      onFeedback: () => {},
      aggregates: {
        'frontend:metric:current:1': 93453.15844726562
      },
      anomalyRange: [0, 0]
    });

    await render(hbs`
      {{rootcause-anomaly
        entities=entities
        aggregates=aggregates
        anomalyUrns=anomalyUrns
        onFeedback=(action onFeedback)
        anomalyRange=anomalyRange
      }}
    `);

    //await wait();
    assert.ok(this.$('.diffcurrent-alert').length > 0);
  });

  test(`does not display warning if anomaly function value and displayed value
    are not more than 1 percent different`, async function(assert) {
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
              current : [ '93453.15844726562' ],
              aggregateMultiplier : [ '1' ],
              metricId : [ '1' ],
              metric : [ 'metric' ],
              function : [ 'function' ],
              comment : [ '' ],
              metricGranularity : [ '1_DAYS' ],
              dataset : [ 'my_dataset' ],
              status : [ 'NO_FEEDBACK' ]
            },
            start: 0,
            end: 0
          }
        },
        anomalyUrns: ['thirdeye:event:anomaly:1', 'thirdeye:metric:1'],
        onFeedback: () => {},
        aggregates: {
          'frontend:metric:current:1': 93453.15844726562
        },
        anomalyRange: [0, 0]
      });

      await render(hbs`
        {{rootcause-anomaly
          entities=entities
          aggregates=aggregates
          anomalyUrns=anomalyUrns
          onFeedback=(action onFeedback)
          anomalyRange=anomalyRange
        }}
      `);

    assert.notOk(this.$('.diffcurrent-alert').length > 0);
  });

  test(`does not display warning if context anomaly range is different than
    anomaly start and end, even when data differs by more than 1 percent`,
    async function(assert) {
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
              metricGranularity : [ '1_DAYS' ],
              dataset : [ 'my_dataset' ],
              status : [ 'NO_FEEDBACK' ]
            },
            start: 0,
            end: 0
          }
        },
        anomalyUrns: ['thirdeye:event:anomaly:1', 'thirdeye:metric:1'],
        onFeedback: () => {},
        aggregates: {
          'frontend:metric:current:1': 93453.15844726562
        },
        anomalyRange: [0, 1]
      });

      await render(hbs`
        {{rootcause-anomaly
          entities=entities
          aggregates=aggregates
          anomalyUrns=anomalyUrns
          onFeedback=(action onFeedback)
          anomalyRange=anomalyRange
        }}
      `);

    assert.notOk(this.$('.diffcurrent-alert').length > 0);
  });
});
