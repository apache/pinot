import { Faker } from 'ember-cli-mirage';

export default function() {

  // These comments are here to help you get started. Feel free to delete them.

  /*
    Config (with defaults).

    Note: these only affect routes defined *after* them!
  */

  // this.urlPrefix = '';    // make this `http://localhost:8080`, for example, if your API is on a different server
  // this.namespace = '';    // make this `/api`, for example, if your API is namespaced
  // this.timing = 400;      // delay for each request, automatically set to 0 during testing

  /*
    Shorthand cheatsheet:

    this.get('/posts');
    this.post('/posts');
    this.get('/posts/:id');
    this.put('/posts/:id'); // or this.patch
    this.del('/posts/:id');

    http://www.ember-cli-mirage.com/docs/v0.3.x/shorthands/
  */

  this.get('/anomalies/search/anomalyIds/1492498800000/1492585200000/:id', (schema, request) => {
    const { id } = request.params;

    return {
      anomalyDetailsList: [{
        anomalyId: id,
        anomalyFunctionName: 'example_anomaly_name',
        currentStart: '2017-04-19 01:00',
        currentEnd: '2017-04-20 02:00',
        dates: ['2017-04-19 01:00', '2017-04-19 02:00', '2017-04-20 01:00', '2017-04-20 02:00'],
        anomalyRegionStart:'2017-04-19 02:00',
        anomalyRegionEnd: '2017-04-20 01:00',
        baseline: 1,
        current: 2,
        baselineValues: [1.0761083176816282E10, 1.0761083176816282E10, 1.1099807067185179E10, 1.1099807067185179E10],
        currentValues: [1.0761083176816282E10, 1.0761083176816282E10, 1.1099807067185179E10, 1.1099807067185179E10]
      }]
    };
  });
}
