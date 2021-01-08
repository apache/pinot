import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';

moduleForComponent(
  'composite-anomalies/parent-anomalies',
  'Integration | Component | composite anomalies/parent anomalies',
  {
    integration: true
  }
);

test('it renders', function (assert) {
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.setProperties({
    tableTitle: 'Composite Anomalies',
    noAnmalies: 'No Composite Anomalies found',
    tableData: [
      {
        id: 1,
        startTime: 1599462000000,
        endTime: 1599721200000,
        feedback: null,
        details: {
          oe_viral_detection: 2,
          feed_feature_coverage_health: 2,
          feed_feature_distribution_health: 2
        }
      }
    ],
    feedbackOptionNames: anomalyUtil.anomalyResponseObj.mapBy('name'),
    feedbackOptionValues: anomalyUtil.anomalyResponseObj.mapBy('value')
  });

  this.render(hbs`{{composite-anomalies/parent-anomalies}}`);

  assert.equal(this.$('h4.panel-title').html().trim(), this.tableTitle);
  assert.equal(this.$('p.composite-anomalies-no-records').html().trim(), this.noAnmalies);

  this.render(hbs`
    {{composite-anomalies/parent-anomalies title=tableTitle}}
  `);

  assert.equal(this.$('.panel-title').html().trim(), this.tableTitle);
  assert.equal(this.$('.composite-anomalies-no-records').html().trim(), this.noAnmalies);

  this.render(hbs`
    {{composite-anomalies/parent-anomalies data=tableData}}
  `);

  // assert.equal($('.start-time').html(), 'Sep 7th, 12:00 ');
  assert.equal(this.$('.te-anomaly-table__duration').html(), '72 hours');
  assert.equal(this.$('.te-anomaly-table__details').html(), 'oe_viral_detection (2)');
  assert.equal(this.$('.ember-power-select-selected-item').html().trim(), this.feedbackOptionNames[0]);

  // Check other values based on feedback

  this.feedbackOptionNames.forEach((option, index) => {
    this.tableData[0].feedback = this.feedbackOptionValues[index];

    this.render(hbs`
      {{composite-anomalies/parent-anomalies data=tableData}}
    `);

    assert.equal(this.$('.ember-power-select-selected-item').html().trim(), option);
  });
});
