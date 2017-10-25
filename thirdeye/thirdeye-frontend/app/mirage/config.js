import queryRelatedMetrics from 'thirdeye-frontend/mocks/queryRelatedMetrics';
import anomalyFunction from 'thirdeye-frontend/mocks/anomalyFunction';
import alertConfig from 'thirdeye-frontend/mocks/alertConfig';
import entityApplication from 'thirdeye-frontend/mocks/entityApplication';
import metric from 'thirdeye-frontend/mocks/metric';
import timeseriesCompare from 'thirdeye-frontend/mocks/timeseriesCompare';

export default function() {

  this.timing = 1000;      // delay for each request, automatically set to 0 during testing

  /**
   * Mocks anomaly data end points
   */
  this.get('/anomalies/search/anomalyIds/1/1/1', (server) => {
    const anomaly = Object.assign({}, server.anomalies.first().attrs);
    const anomalyDetailsList = [ anomaly ];
    return { anomalyDetailsList };
  });

  /**
   * Mocks related Metric Id endpoints
   */
  this.get('/rootcause/queryRelatedMetrics', () => {
    return queryRelatedMetrics;
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
    };
  });

  /**
   * Mocks a list of alerts, displayed in the /manage/alerts page
   */
  this.get('/thirdeye/entity/ANOMALY_FUNCTION', () => {
    return anomalyFunction;
  });

  /**
   * Mocks email subscription groups for alerts
   */
  this.get('/thirdeye/entity/ALERT_CONFIG', () => {
    return alertConfig;
  });

  /**
   * Mocks a list of applications that are onboarded onto ThirdEye
   */
  this.get('/thirdeye/entity/APPLICATION', () => {
    return entityApplication;
  });

  /**
   * Returns information about the first alert in anomalyFunction mock data
   */
  this.get(`/onboard/function/${anomalyFunction[0].id}`, () => {
    return anomalyFunction[0];
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
   * Returns the first email config
   */
  this.get(`/thirdeye/email/function/${anomalyFunction[0].id}`, () => {
    return alertConfig[0];
  });

  /**
   * Returns mock data to render time series graph
   */
  this.get(`/timeseries/compare/${metric[0].id}/***`, () => {
    return timeseriesCompare;
  });
}
