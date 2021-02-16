import queryRelatedMetrics from 'thirdeye-frontend/mocks/queryRelatedMetrics';
import metric from 'thirdeye-frontend/mocks/metric';
import timeseriesCompare from 'thirdeye-frontend/mocks/timeseriesCompare';
import { filters, dimensions, granularities } from 'thirdeye-frontend/mocks/metricPeripherals';
import rootcause from './endpoints/rootcause';
import selfserve from './endpoints/selfserve';
import auth from './endpoints/auth';
import entityMapping from './endpoints/entity-mapping';

export default function () {
  this.timing = 1000; // delay for each request, automatically set to 0 during testing

  /**
   * Mocks anomaly data end points
   */
  this.get('/anomalies/search/anomalyIds/1/1/1', (server) => {
    const anomaly = Object.assign({}, server.anomalies.first().attrs);
    const anomalyDetailsList = [anomaly];
    return { anomalyDetailsList };
  });

  /**
   * Mocks related Metric Id endpoints
   */
  this.get('/rootcause/queryRelatedMetrics', () => {
    return queryRelatedMetrics;
  });

  /**
   * Mocks related Metric Id endpoints
   */
  // this.get('/rootcause/metric/aggregate/batch', (server, request) => {
  //   const { urn, start, end, offsets, timezone } = request.queryParams;
  //   return end;
  // });

  /**
   * Mocks anomaly region endpoint
   */
  this.get('/data/anomalies/ranges', (server, request) => {
    const { metricIds, start, end } = request.queryParams;

    const regions = metricIds.split(',').reduce((regions, id) => {
      regions[id] = [
        {
          start,
          end
        }
      ];

      return regions;
    }, {});

    return regions;
  });

  /**
   * Mocks time series compare endpoints
   */
  this.get('/timeseries/compare/:id/:currentStart/:currentEnd/:baselineStart/:baselineEnd', (server, request) => {
    const { id, currentStart, currentEnd } = request.params;
    const interval = 3600000;
    const dataPoint = Math.floor((+currentEnd - currentStart) / interval);

    //TODO: mock data dynamically
    return {
      metricName: 'example Metric',
      metricId: id,
      start: currentStart,
      end: currentEnd,

      timeBucketsCurrent: [...new Array(dataPoint)].map((point, index) => {
        return +currentStart + index * interval;
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
            const num = Math.random() * 200 - 100;
            return num.toFixed(2);
          })
        }
      }
    };
  });

  /**
   * Returns metric information about the first alert
   */
  this.get('/data/autocomplete/***', () => {
    return metric;
  });

  /**
   * Returns max data time.
   */
  this.get(`/data/maxDataTime/metricId/${metric[0].id}`, () => {
    return 1509051599998;
  });

  /**
   * Returns metric granularity.
   */
  this.get(`/data/agg/granularity/metric/${metric[0].id}`, () => {
    return granularities;
  });

  /**
   * Returns available filters on this metric
   */
  this.get(`/data/autocomplete/filters/metric/${metric[0].id}`, () => {
    return filters;
  });

  /**
   * Returns available dimensions for this metric
   */
  this.get(`/data/autocomplete/dimensions/metric/${metric[0].id}`, () => {
    return dimensions;
  });

  /**
   * Returns mock data to render time series graph
   */
  this.get(`/timeseries/compare/${metric[0].id}/***`, () => {
    return timeseriesCompare;
  });

  rootcause(this);
  selfserve(this);
  auth(this);
  entityMapping(this);
}
