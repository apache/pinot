import { faker } from 'ember-cli-mirage';
import moment from 'moment';

export default function() {

  // These comments are here to help you get started. Feel free to delete them.

  /*
    Config (with defaults).

    Note: these only affect routes defined *after* them!
  */

  // this.urlPrefix = '';    // make this `http://localhost:8080`, for example, if your API is on a different server
  // this.namespace = '';    // make this `/api`, for example, if your API is namespaced
  this.timing = 400;      // delay for each request, automatically set to 0 during testing

  /*
    Shorthand cheatsheet:`

    this.get('/posts');
    this.post('/posts');
    this.get('/posts/:id');
    this.put('/posts/:id'); // or this.patch
    this.del('/posts/:id');

    http://www.ember-cli-mirage.com/docs/v0.3.x/shorthands/
  */

  this.get('/anomalies/search/anomalyIds/1492498800000/1492585200000/1', (schema, request) => {
    const { anomalyIds } = request.queryParams;
    // TODO: move this to a utils/helper file
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
    }

     // TODO: move this to a utils/helper file
    const makeValues = (dataPointsNum = 100) => {
      const valueArray = [];
      let i = 0;
      while (i++ < dataPointsNum) {
        let newValue = faker.random.number({min:1, max:100})
        valueArray.push(newValue);
      }
      return valueArray; 
    }

    const dateArray = makeDates();
    const valueArray = makeValues();
    return {
      anomalyDetailsList: [{
        anomalyIds,
        anomalyFunctionName: 'example_anomaly_name',
        currentStart: dateArray[0],
        currentEnd: dateArray[dateArray.length-1],
        dates: dateArray,
        anomalyRegionStart: dateArray[40],
        anomalyRegionEnd: dateArray[50],
        baseline: 1,
        current: 2,
        baselineValues: valueArray,
        currentValues: valueArray.map(value => value + 20),
      }]
    };
  });
}
