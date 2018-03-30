import metrics from 'thirdeye-frontend/mocks/metric';
import anomalySet from 'thirdeye-frontend/mocks/anomalies';
import alertConfig from 'thirdeye-frontend/mocks/alertConfig';
import entityApplication from 'thirdeye-frontend/mocks/entityApplication';
import anomalyChangeData from 'thirdeye-frontend/mocks/anomalyWowChange';
import anomalyPerformanceData from 'thirdeye-frontend/mocks/anomalyPerformance';
import { selfServeSettings } from 'thirdeye-frontend/tests/utils/constants';

export default function (server) {

  server.loadFixtures();

  /**
   * get request for dashboard name verification
   */
  server.get('autometrics/isIngraphDashboard/:name', (schema, request) => {
    return request.params.name === 'thirdeye-all' ? true : false;
  });

  /**
   * Post request for creating a new dataset config in TE
   */
  server.post('/onboard/create', () => {
    return 'Created config with id 28863147';
  });

  /**
   * Post request for auto-onboarding newly imported metrics
   */
  server.post('/autoOnboard/runAdhoc/AutometricsThirdeyeDataSource', () => {
    return {};
  });

  /**
   * get request for metric import verification
   */
  server.get('/thirdeye-admin/metric-config/metrics', () => {
    return {
      "Result":"OK",
      "Records":[
      "thirdeye-demo::thirdeye_rec1",
      "thirdeye-demo::Thirdeye_rec2",
      "thirdeye-demo::Thirdeye_rec3",
      "thirdeye-demo::Thirdeye_rec4",
      "thirdeye-demo::Thirdeye_rec5"
      ]
    };
  });

  /**
   * Mocks email subscription groups for alerts
   */
  server.get('/thirdeye/entity/ALERT_CONFIG', () => {
    return alertConfig;
  });

  /**
   * Mocks a list of applications that are onboarded onto ThirdEye
   */
  server.get('/thirdeye/entity/APPLICATION', () => {
    return entityApplication;
  });

  /**
   * Post request for editing alert
   */
  server.post(`/thirdeye/entity`, (schema, request) => {
    const params = request.queryParams && request.queryParams.entityType;

    if (params === 'ANOMALY_FUNCTION') {
      const requestBody = JSON.parse(request.requestBody);
      const id = requestBody.id;
      return schema.db.alerts.update(id, requestBody);
    }
  });

  /**
   * Returns the email config by id
   */
  server.get(`/thirdeye/email/function/:id`, (schema, request) => {
    return [alertConfig[1]];
  });

  /**
   * Returns information about an alert by id in anomalyFunction mock data
   */
  server.get(`/onboard/function/:id`, (schema, request) => {
    return schema.alerts.find(request.params.id);
  });

  /**
   * Create alert: POST request to create alert container in DB
   */
  server.post('/function-onboard/create-function', (schema, request) => {
    // Update the test DB alert record with the new name. This endpoint generates the alert.
    server.db.alerts.update({ functionName: request.queryParams.name });
    return server.db.alerts.find(1);
  });

  /**
   * Create alert: POST request to update an existing alert function with properties payload
   * The response can be simulated as 'success' or 'failure'
   */
  server.post('/detection-onboard/create-job', (schema, request) => {
    server.db.jobs.update({
      jobName: request.queryParams.jobName,
      taskStatuses: [],
      jobStatus: 'SCHEDULED'
    });

    return server.db.jobs.find(1);
  });

  /**
   * Create alert: GET request for job status, with the ability to simulate failure of specific tasks
   * Note: to simulate a pending replay job, set jobStatus = 'SCHEDULED' and replayTaskStatus = 'RUNNING'
   */
  server.get('/detection-onboard/get-status', (schema, request) => {
    const jobId = request.queryParams.jobId;
    const jobStatus = 'COMPLETED';
    const taskStatus = 'COMPLETED';

    return server.db.jobs.update(jobId, {
      jobStatus,
      taskStatuses:[
        { taskName: 'DataPreparation', taskStatus },
        { taskName: 'FunctionAlertCreation', taskStatus },
        { taskName: 'FunctionReplay', taskStatus }
      ]
    });
  });

  /**
   * Mocks a list of alerts, displayed in the /manage/alerts page
   */
  server.get('/thirdeye/entity/ANOMALY_FUNCTION', (schema) => {
    return schema.alerts.all().models;
  });

  /**
   * get request for metric import verification
   */
  server.get('/detection-job/eval/:type/:id', () => {
    return anomalyPerformanceData;
  });

  /**
   * get request for metric import verification
   */
  server.get('/data/autocomplete/metric/test', () => {
    return metrics;
  });

  /**
   * get request for metric import verification
   */
  server.get('/dashboard/anomaly-function/:id/anomalies', () => {
    return [ 38456269, 382977991, 38293331 ];
  });

  /**
   * get request for metric import verification
   */
  server.get('/dashboard/anomalies/score/:id', () => {
    return 0.5062645533578358;
  });

  /**
   * get request for metric import verification
   */
  server.get('/anomalies/:id', () => {
    return anomalyChangeData;
  });

  /**
   * get request for metric import verification
   */
  server.get('/anomalies/search/anomalyIds/0/0/1', () => {
    return anomalySet;
  });
}
