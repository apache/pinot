import queryRelatedMetrics from 'thirdeye-frontend/mocks/queryRelatedMetrics';
import alertConfig from 'thirdeye-frontend/mocks/alertConfig';
import entityApplication from 'thirdeye-frontend/mocks/entityApplication';
import metric from 'thirdeye-frontend/mocks/metric';
import timeseriesCompare from 'thirdeye-frontend/mocks/timeseriesCompare';
import { onboardJobStatus, onboardJobCreate } from 'thirdeye-frontend/mocks/detectionOnboard';
import rootcause from 'thirdeye-frontend/mirage/endpoints/rootcause';

/**
 * TODO: Group endpoints together and put them in files under the endpoints folder to prevent overloading this file
 */

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
  this.get('/thirdeye/entity/ANOMALY_FUNCTION', (schema) => {
    return schema.alerts.all().models;
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
   * Returns information about an alert by id in anomalyFunction mock data
   */
  this.get(`/onboard/function/:id`, (schema, request) => {
    return schema.alerts.find(request.params.id);
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
    return [ "5_MINUTES", "HOURS", "DAYS" ];
  });

  /**
   * Returns available filters on this metric
   */
  this.get(`/data/autocomplete/filters/metric/${metric[0].id}`, () => {
    return {
      "container" : [ "container1", "container2" ],
      "fabric" : [ "prod-xyz1", "prod-xyz2", "prod-xyz3" ]
    };
  });

  /**
   * Returns available dimensions for this metric
   */
  this.get(`/data/autocomplete/dimensions/metric/${metric[0].id}`, () => {
    return [ "All", "fabric", "container", "host" ];
  });

  /**
   * Returns job status
   */
  this.post(`/detection-onboard/get-status`, (schema, request) => {
    return onboardJobStatus;
  });

  /**
   * Returns job id
   */
  this.post(`/detection-onboard/create-job`, (schema, request) => {
    return onboardJobCreate;
  });

  /**
   * Returns the email config by id
   */
  this.get(`/thirdeye/email/function/:id`, (schema, request) => {
    return [alertConfig[request.params.id]];
  });

  /**
   * Returns mock data to render time series graph
   */
  this.get(`/timeseries/compare/${metric[0].id}/***`, () => {
    return timeseriesCompare;
  });

  /**
   * Post request for editing alert
   */
  this.post(`/thirdeye/entity`, (schema, request) => {
    const params = request.queryParams && request.queryParams.entityType;

    if (params === 'ANOMALY_FUNCTION') {
      const requestBody = JSON.parse(request.requestBody);
      const id = requestBody.id;
      return schema.db.alerts.update(id, requestBody);
    }
  });

  rootcause(this);
}
