import { Factory, faker } from 'ember-cli-mirage';
import moment from 'moment';

/**
 * Builds an Array of dates
 * @param {Number}    dataPointsNum Data points needed
 * @return {[String]}               Array of DateString
 */
const makeDates = (dataPointsNum = 100) => {
  let i = 0;
  const dateArray = [];
  const startDate = moment().subtract(14, 'days');
  while (i++ < dataPointsNum) {
    let newDate = startDate.clone().format('YYYY-MM-DD HH:MM');
    dateArray.push(newDate);
    startDate.add(1, 'hours');
  }
  return dateArray;
};

/**
 * Builds an Array of random values
 * @param {Number}    dataPointsNum Data points needed
 * @return {[String]}               Array of DateString
 */
const makeValues = (dataPointsNum = 100) => {
  const valueArray = [];
  let i = 0;
  while (i++ < dataPointsNum) {
    let newValue = faker.random.number({min:1, max:100});
    valueArray.push(newValue);
  }
  return valueArray;
};

let dateArray = makeDates();
let valueArray = makeValues();

export default Factory.extend({
  anomalyFunctionName: 'example_metric_name',
  currentStart: dateArray[0],
  currentEnd: dateArray[dateArray.length-1],
  dates: dateArray,
  anomalyRegionStart: dateArray[40],
  anomalyRegionEnd: dateArray[50],
  baseline: 1,
  current: 2,
  metricId: 1234,
  baselineValues: valueArray,
  currentValues() {
    return this.baselineValues.map(value => {
      return value + faker.random.number({min:-50, max:50});
    });
  }
});
