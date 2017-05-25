import { Factory } from 'ember-cli-mirage';

export default Factory.extend({
  anomalyFunctionName: 'example_anomaly_name',
  currentStart: '2017-04-19 02:00',
  currentEnd: '2017-04-20 01:00',
  dates: ['2017-04-19 01:00', '2017-04-19 02:00', '2017-04-20 01:00', '2017-04-20 02:00'],
  baseline: 1,
  current: 2,
  baselineValues: [1.0761083176816282E10, 1.0761083176816282E10, 1.1099807067185179E10, 1.1099807067185179E10],
  currentValues: [1.0761083176816282E10, 1.0761083176816282E10, 1.1099807067185179E10, 1.1099807067185179E10]
});