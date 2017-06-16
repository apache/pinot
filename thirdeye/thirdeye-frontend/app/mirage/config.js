export default function() {

  // These comments are here to help you get started. Feel free to delete them.

  /*
    Config (with defaults).

    Note: these only affect routes defined *after* them!
  */

  // this.urlPrefix = '';    // make this `http://localhost:8080`, for example, if your API is on a different server
  // this.namespace = '';    // make this `/api`, for example, if your API is namespaced
  this.timing = 1000;      // delay for each request, automatically set to 0 during testing

  /*
    Shorthand cheatsheet:`

    this.get('/posts');
    this.post('/posts');
    this.get('/posts/:id');
    this.put('/posts/:id'); // or this.patch
    this.del('/posts/:id');

    http://www.ember-cli-mirage.com/docs/v0.3.x/shorthands/
  */

  /**
   * Mocks anomaly data end points
   */
  this.get('/anomalies/search/anomalyIds/1492498800000/1492585200000/1', (server) => {
    const anomaly = Object.assign({}, server.anomalies.first().attrs);
    const anomalyDetailsList = [ anomaly ];
    return { anomalyDetailsList };
  });

  /**
   * Mocks related Metric Id endpoints
   */
  this.get('/rootcause/queryRelatedMetrics', () => {
    return [{
      urn: "thirdeye:metric:1234",
      score: 0.955,
      label: "exampleMetric",
      type: "Metric",
    }]
  });

  /**
   * Mocks anomaly region endpoint
   */
  this.get('/data/anomalies/ranges', (server, request) => {
    const { metricIds, start, end } = request.queryParams;

    const regions = metricIds
      .split(',')
      .reduce((regions, id) => {
        regions[id] = [{
          start,
          end
        }];

        return regions;
      }, {})

    return regions;
  })

  /**
   * Mocks time series compare endpoints 
   */
  this.get('/timeseries/compare/:id/:currentStart/:currentEnd/:baselineStart/:baselineEnd', (server, request) => {
    const { id, currentStart, currentEnd } = request.params;
    const interval = 3600000;
    const dataPoint = Math.floor((+currentEnd - currentStart) / interval);

    //TODO: mock data dynamically
    return {
      metricName: "example Metric",
      metricId: id,
      start: currentStart,
      end: currentEnd,

      timeBucketsCurrent: [...new Array(dataPoint)].map((point, index) => {
        return +currentStart + (index * interval);
      }),
      
      subDimensionContributionMap: {
        All: {
          currentValues: [...new Array(dataPoint)].map(() => {
            const num = Math.random() * 100;
            return num.toFixed(2);
          }),
          baselineValues: [...new Array(dataPoint)].map(() => {
            const num = Math.random() * 100;
            return num.toFixed(2);
          }),
          percentageChange: [...new Array(dataPoint)].map(() => {
            const num = (Math.random() * 200) - 100;
            return num.toFixed(2);
          })
        }
      }
    }
  });
}
