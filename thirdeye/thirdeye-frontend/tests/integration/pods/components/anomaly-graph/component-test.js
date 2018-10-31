// TODO: [THIRDEYE-1796] Fix failing tests
// import { moduleForComponent, test } from 'ember-qunit';
// import hbs from 'htmlbars-inline-precompile';

// moduleForComponent('anomaly-graph', 'Integration | Component | anomaly graph', {
//   integration: true
// });

// test('it renders', function(assert) {
//   const selector = '.anomaly-graph';
//   const anomaly = {
//     dates: ['2017-01-01 10:00', '2017-01-02 10:00', '2017-01-03 10:00'], 
//     currentValues: [1,2,3],
//     baselineValues: [4,5,6],
//     anomalyRegionStart: '2017-01-01 10:00',
//     anomalyRegionEnd: '2017-01-03 10:00',
//     metricName: 'test_metric'
//   };
//   this.set('anomaly', anomaly);
//   this.render(hbs`{{anomaly-graph anomaly=anomaly}}`);

//   assert.ok(this.$(selector).length, 'anomaly graph renders correctly');
// });
