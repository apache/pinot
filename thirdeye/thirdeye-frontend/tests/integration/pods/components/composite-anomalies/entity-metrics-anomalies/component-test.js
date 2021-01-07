import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';

moduleForComponent(
  'composite-anomalies/entity-metrics-anomalies',
  'Integration | Component | composite anomalies/entity metrics anomalies',
  {
    integration: true
  }
);

test('it renders', function (assert) {
  this.setProperties({
    tableTitle: 'ENTITY',
    tableData: [
      {
        id: 9,
        startTime: 1599462000000,
        endTime: 1599721200000,
        feedback: null,
        metric: 'metric_four',
        dimensions: {
          feature_name: 'groupConstituentOne#',
          feature_section: 'groupConstituentOne',
          dimension_three: 'True',
          use_case: 'DESKTOP'
        },
        currentPredicted: {
          current: '4.00',
          predicted: '2.00',
          deviation: 1,
          deviationPercent: '+100.0%'
        }
      }
    ],
    feedbackOptionNames: anomalyUtil.anomalyResponseObj.mapBy('name'),
    feedbackOptionValues: anomalyUtil.anomalyResponseObj.mapBy('value')
  });

  this.render(hbs`
    {{composite-anomalies/entity-metrics-anomalies title=tableTitle data=tableData}}
  `);

  assert.equal(this.$('.panel-title').html().trim(), this.tableTitle);

  assert.equal(this.$('.te-anomaly-table__duration')[0].innerHTML, '72 hours');

  assert.equal(this.$('.te-anomaly-table__metric')[0].innerHTML.trim(), 'metric_four');

  assert.equal(this.$('.te-anomaly-table__dimension')[0].innerHTML.trim(), 'feature_name:groupConstituentOne#');
  assert.equal(this.$('.te-anomaly-table__dimension')[1].innerHTML.trim(), 'feature_section:groupConstituentOne');
  assert.equal(this.$('.te-anomaly-table__dimension')[2].innerHTML.trim(), 'dimension_three:True');
  assert.equal(this.$('.te-anomaly-table__dimension')[3].innerHTML.trim(), 'use_case:DESKTOP');

  assert.equal(this.$('.te-anomaly-table__current-baseline')[0].innerHTML.trim(), '4.00/2.00');
  assert.equal(this.$('.te-anomaly-table__deviation-percent')[0].innerHTML.trim(), '(+100.0%)');

  assert.equal(this.$('.ember-power-select-selected-item').html().trim(), this.feedbackOptionNames[0]);

  // Check other values based on feedback

  this.feedbackOptionNames.forEach((option, index) => {
    this.tableData[0].feedback = this.feedbackOptionValues[index];

    this.render(hbs`
      {{composite-anomalies/entity-metrics-anomalies data=tableData}}
    `);

    assert.equal(this.$('.ember-power-select-selected-item').html().trim(), option);
  });
});
