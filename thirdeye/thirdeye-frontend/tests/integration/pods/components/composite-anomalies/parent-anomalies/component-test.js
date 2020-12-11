import $ from 'jquery';
import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

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
    feedbackOptions: [
      'Not reviewed yet',
      'Yes - unexpected',
      'Expected temporary change',
      'Expected permanent change',
      'No change observed'
    ]
  });

  this.render(hbs`{{composite-anomalies/parent-anomalies}}`);

  assert.equal($('h4.panel-title').html(), this.tableTitle);
  assert.equal($('p.composite-anomalies-no-records').html(), this.noAnmalies);

  this.render(hbs`
    {{composite-anomalies/parent-anomalies title=tableTitle}}
  `);

  assert.equal($('.panel-title').html(), this.tableTitle);
  assert.equal($('.composite-anomalies-no-records').html(), this.noAnmalies);

  this.render(hbs`
    {{composite-anomalies/parent-anomalies data=tableData}}
  `);

  assert.equal($('.start-time').html(), 'Sep 7th, 12:00 ');
  assert.equal($('.duration').html(), '72 hours');
  assert.equal($('.details').html(), 'oe_viral_detection (2)');
  assert.equal($('.ember-power-select-selected-item').html().trim(), this.feedbackOptions[0]);

  // Check other values based on feedback

  this.feedbackOptions.forEach((option, index) => {
    this.tableData[0].feedback = index === 0 ? null : index;

    this.render(hbs`
      {{composite-anomalies/parent-anomalies data=tableData}}
    `);

    assert.equal($('.ember-power-select-selected-item').html().trim(), option);
  });
});
