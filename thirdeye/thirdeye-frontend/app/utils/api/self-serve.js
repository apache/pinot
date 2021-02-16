/**
 * GET request for job status (a sequence of events including create, replay, autotune)
 * Requires 'jobId' query param
 * @see {@link https://tinyurl.com/ya6mwyjt|class DetectionOnboardResource}
 */
const jobStatus = (jobId) => `/detection-onboard/get-status?jobId=${jobId}`;

/**
 * POST request to create alert container in DB
 * Requires 'name' query param
 * @param {String} newAlertName: Name of new alert function
 * @see {@link https://tinyurl.com/yb4ebnbd|class FunctionOnboardingResource}
 */
const createAlert = (newAlertName) => `/function-onboard/create-function?name=${newAlertName}`;

/**
 * POST request to update an existing alert function with properties payload
 * Requires 'payload' object in request body and 'jobName' in query param
 * @param {String} jobName: unique name of new job for alert setup task
 * @see {@link https://tinyurl.com/yd84uj2b|class DetectionOnboardResource}
 */
const updateAlert = (jobName) => `/detection-onboard/create-job?jobName=${jobName}`;

/**
 * DELETE request to remove an alert function from the DB
 * Requires query param 'id'
 * @param {String} functionId: id of alert to remove
 * @see {@link https://tinyurl.com/ybmgmzhd|class AnomalyResource}
 */
const deleteAlert = (functionId) => `/dashboard/anomaly-function?id=${functionId}`;

/**
 * GET a single function record by id
 * @param {String} id: alert function id
 * @see {@link https://tinyurl.com/y76fu5vp|class OnboardResource}
 */
const getAlertById = (functionId) => `/onboard/function/${functionId}`;

/**
 * GET the timestamp for the end of the time range of available data for a given metric
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/y8vxqvg7|class DataResource}
 */
const maxDataTime = (metricId) => `/data/maxDataTime/metricId/${metricId}`;

/**
 * GET the timestamp for the end of the time range of available data for a given metric
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/y8vxqvg7|class DataResource}
 */
const metricGranularity = (metricId) => `/data/agg/granularity/metric/${metricId}`;

/**
 * GET the all filters associated with this metric
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/y7o864cq|class DataResource}
 */
const metricFilters = (metricId) => `/data/autocomplete/filters/metric/${metricId}`;

/**
 * GET a list of dimensions by metric.
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/yca7j4hz|class DataResource}
 */
const metricDimensions = (metricId) => `/data/autocomplete/dimensions/metric/${metricId}`;

/**
 * GET | Validates whether the entered dashboard name exists in inGraphs
 * @param {String} dashboardName
 */
const dashboardByName = (dashboardName, fabricGroup) =>
  `/autometrics/isIngraphDashboard/${dashboardName}?fabricGroup=${fabricGroup}`;

/**
 * GET all metrics that belong to a dataset
 * @param {String} dataSet
 * @see {@link https://tinyurl.com/yb34ubfd|class MetricConfigResource}
 */
const metricsByDataset = (dataSet) => `/thirdeye-admin/metric-config/metrics?dataset=${dataSet}`;

/**
 * POST | Trigger onboarding task of all new imported metrics
 * @see {@link https://tinyurl.com/y9mezgdr|class AutoOnboardResource}
 */
const triggerInstantOnboard = '/autoOnboard/runAdhoc/AutometricsThirdeyeDataSource';

/**
 * POST request to edit an existing alert function with properties payload
 */
const editAlert = '/thirdeye/entity?entityType=ANOMALY_FUNCTION';

/**
 * POST | Add metric & dataset to thirdEye DB
 * Requires payload containing: 'datasetName', 'metricName', 'dataSource', 'properties'
 * @see {@link https://tinyurl.com/y9g77y87|class OnboardDatasetMetricResource}
 */
const createNewDataset = '/onboard/create';

/**
 * PUT | Set active status of alert
 * @param {Number} detectionId
 * @param {Boolean} active
 */
const setAlertActivationUrl = (detectionId, active) => `/yaml/activation/${detectionId}?active=${active}`;

/**
 * GET autocomplete request for alert by name
 * @param {String} functionName: alert name
 * @see {@link https://tinyurl.com/ybelagey|class DataResource}
 */
const alertByName = (functionName) => `/data/autocomplete/detection?name=${encodeURIComponent(functionName)}`;

/**
 * GET autocomplete request for application
 * @param {String} app: application name
 */
const application = (app) => `/data/autocomplete/application?name=${encodeURIComponent(app)}`;

/**
 * GET autocomplete request for dataset
 * @param {String} type: dataset name
 */
const dataset = (name) => `/data/autocomplete/dataset?name=${encodeURIComponent(name)}`;

/**
 * GET autocomplete request for detection type
 * @param {String} type: detection type
 */
const detectionType = (type) => `/data/autocomplete/ruleType?name=${encodeURIComponent(type)}`;

/**
 * GET autocomplete request for metric data by name
 * @param {String} metricName: metric name
 * @see {@link https://tinyurl.com/y7hhzm33|class DataResource}
 */
const metric = (metricName) => `/data/autocomplete/metric?name=${encodeURIComponent(metricName)}`;

/**
 * GET autocomplete request for alerty owner
 * @param {String} name: owner name
 */
const owner = (name) => `/data/autocomplete/detection-createdby?name=${encodeURIComponent(name)}`;

/**
 * GET autocomplete request for subscription group by name
 * @param {String} name: group name
 */
const subscriptionGroup = (name) => `/data/autocomplete/subscription?name=${encodeURIComponent(name)}`;

/**
 * GET alerts, filtered and paginated according to params
 * @param {Object} params: params to query alerts by
 */
const getPaginatedAlertsUrl = (params) => {
  const paramKeys = Object.keys(params || {});
  let url = '/alerts';
  if (paramKeys.length === 0) {
    return url;
  }
  url += '?';
  paramKeys.forEach((key) => {
    if (Array.isArray(params[key])) {
      const paramVals = params[key];
      paramVals.forEach((value) => {
        url += `${key}=${encodeURIComponent(value)}`;
        url += '&';
      });
      url = url.slice(0, -1);
    } else {
      url += `${key}=${encodeURIComponent(params[key])}`;
    }
    url += '&';
  });
  url = url.slice(0, -1);
  return url;
};

/**
 * Autocomplete endpoints
 */
export const autocompleteAPI = {
  alertByName,
  application,
  detectionType,
  dataset,
  getAlertById,
  metric,
  owner,
  subscriptionGroup
};

/**
 * Yaml endpoints
 */
export const yamlAPI = {
  setAlertActivationUrl,
  getPaginatedAlertsUrl
};

/**
 * Graph-related self-serve endpoints
 */
export const selfServeApiGraph = {
  maxDataTime,
  metricGranularity,
  metricDimensions,
  metricFilters
};

/**
 * Onboarding endpoints for self-serve
 */
export const selfServeApiOnboard = {
  jobStatus,
  createAlert,
  updateAlert,
  deleteAlert,
  editAlert,
  dashboardByName,
  metricsByDataset,
  createNewDataset,
  triggerInstantOnboard
};

export default {
  autocompleteAPI,
  selfServeApiOnboard,
  selfServeApiGraph
};
