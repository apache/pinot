import metrics from 'thirdeye-frontend/mocks/metric';
import { anomalySet } from 'thirdeye-frontend/mocks/anomalies';
import alertConfig from 'thirdeye-frontend/mocks/alertConfig';
import entityApplication from 'thirdeye-frontend/mocks/entityApplication';
import anomalyChangeData from 'thirdeye-frontend/mocks/anomalyWowChange';
import { performanceData } from 'thirdeye-frontend/mocks/anomalyPerformance';

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
      Result: 'OK',
      Records: [
        'thirdeye-demo::thirdeye_rec1',
        'thirdeye-demo::Thirdeye_rec2',
        'thirdeye-demo::Thirdeye_rec3',
        'thirdeye-demo::Thirdeye_rec4',
        'thirdeye-demo::Thirdeye_rec5'
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
  server.post('/thirdeye/entity', (schema, request) => {
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
  server.get('/thirdeye/email/function/:id', () => {
    return [alertConfig[1]];
  });

  /**
   * Returns information about an alert by id in anomalyFunction mock data
   */
  server.get('/onboard/function/:id', (schema, request) => {
    return schema.alerts.find(request.params.id);
  });

  /**
   * Create alert: POST request to create alert container in DB
   */
  server.post('/function-onboard/create-function', (schema, request) => {
    // Update the test DB alert record with the new name. This endpoint generates the alert.
    const newAlert = server.create('alert', { functionName: request.queryParams.name });
    return newAlert;
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
   * Tune alert: POST request to apply tuning filters for anomaly detection
   */
  server.post('/detection-job/autotune/filter/:id', () => {
    return [1234567];
  });

  /**
   * Tune alert: GET request to fetch new mttd value
   */
  server.get('/detection-job/eval/projected/mttd/:id', () => {
    return '0';
  });

  /**
   * Tune alert: GET request to fetch new projected anomalies
   */
  server.get('/detection-job/eval/projected/anomalies/:id', () => {
    return [38456269];
  });

  /**
   * Tune alert: GET request to fetch new perf data
   */
  server.get('/detection-job/eval/autotune/:id', () => {
    return performanceData(1);
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
      taskStatuses: [
        { taskName: 'DataPreparation', taskStatus },
        { taskName: 'FunctionAlertCreation', taskStatus },
        { taskName: 'FunctionReplay', taskStatus }
      ]
    });
  });

  /**
   * Mocks a list of alerts, displayed in the /manage/alerts page
   */
  server.get('/alerts', (schema) => {
    const response = {
      count: schema.alerts.all().models.length,
      elements: schema.alerts.all().models
    };
    return response;
  });

  /**
   * get request for metric import verification
   */
  server.get('/detection-job/eval/:type/:id', () => {
    return performanceData();
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
    return [38456269, 382977991, 38293331];
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
  server.get('/anomalies/search/anomalyIds/0/0/1', (schema, request) => {
    const idArray = request.queryParams.anomalyIds ? request.queryParams.anomalyIds : [38456269];
    return anomalySet(idArray.split(','));
  });

  /**
   * get request for all detection alerters
   */
  server.get('/thirdeye/entity/DETECTION_ALERT_CONFIG', () => {
    return [];
  });

  /**
   * get request for detection config
   */
  server.get('/detection/:id', () => {
    return [];
  });

  /**
   * get request for subscription groups of given alert
   */
  server.get('/detection/subscription-groups/:id', () => {
    return [];
  });
}
