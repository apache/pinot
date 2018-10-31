/**
 * GET request for all alert subscription groups
 * @see {@link https://tinyurl.com/ycv55yyx|class EntityManagerResource}
 */
const allConfigGroups = '/thirdeye/entity/ALERT_CONFIG';

/**
 * GET request for all alert application groups
 * @see {@link https://tinyurl.com/ycv55yyx|class EntityManagerResource}
 */
const allApplications = '/thirdeye/entity/APPLICATION';

/**
 * GET request for job status (a sequence of events including create, replay, autotune)
 * Requires 'jobId' query param
 * @see {@link https://tinyurl.com/ya6mwyjt|class DetectionOnboardResource}
 */
const jobStatus = jobId => `/detection-onboard/get-status?jobId=${jobId}`;

/**
 * POST request to create alert container in DB
 * Requires 'name' query param
 * @param {String} newAlertName: Name of new alert function
 * @see {@link https://tinyurl.com/yb4ebnbd|class FunctionOnboardingResource}
 */
const createAlert = newAlertName => `/function-onboard/create-function?name=${newAlertName}`;

/**
 * POST request to update an existing alert function with properties payload
 * Requires 'payload' object in request body and 'jobName' in query param
 * @param {String} jobName: unique name of new job for alert setup task
 * @see {@link https://tinyurl.com/yd84uj2b|class DetectionOnboardResource}
 */
const updateAlert = jobName => `/detection-onboard/create-job?jobName=${jobName}`;

/**
 * DELETE request to remove an alert function from the DB
 * Requires query param 'id'
 * @param {String} functionId: id of alert to remove
 * @see {@link https://tinyurl.com/ybmgmzhd|class AnomalyResource}
 */
const deleteAlert = functionId => `/dashboard/anomaly-function?id=${functionId}`;

/**
 * GET autocomplete request for metric data by name
 * @param {String} metricName: metric name
 * @see {@link https://tinyurl.com/y7hhzm33|class DataResource}
 */
const metricAutoComplete = metricName => `/data/autocomplete/metric?name=${encodeURIComponent(metricName)}`;

/**
 * GET autocomplete request for alert by name
 * @param {String} functionName: alert name
 * @see {@link https://tinyurl.com/ybelagey|class DataResource}
 */
const alertFunctionByName = functionName => `/data/autocomplete/functionByName?name=${encodeURIComponent(functionName)}`;

/**
 * GET autocomplete request for alert by app name
 * @param {String} appName: application name
 * @see {@link https://tinyurl.com/ybelagey|class DataResource}
 */
const alertFunctionByAppName = appName => `/data/autocomplete/functionByAppname?appname=${encodeURIComponent(appName)}`;

/**
 * GET a single function record by id
 * @param {String} id: alert function id
 * @see {@link https://tinyurl.com/y76fu5vp|class OnboardResource}
 */
const alertById = functionId => `/onboard/function/${functionId}`;

/**
 * GET config group records containing functionId in emailConfig property
 * @param {String} id: alert function id
 * @see {@link https://tinyurl.com/yc36oo2c|class EmailResource}
 */
const configGroupByAlertId = functionId => `/thirdeye/email/function/${functionId}`;

/**
 * GET the timestamp for the end of the time range of available data for a given metric
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/y8vxqvg7|class DataResource}
 */
const maxDataTime = metricId => `/data/maxDataTime/metricId/${metricId}`;

/**
 * GET the timestamp for the end of the time range of available data for a given metric
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/y8vxqvg7|class DataResource}
 */
const metricGranularity = metricId => `/data/agg/granularity/metric/${metricId}`;

/**
 * GET the all filters associated with this metric
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/y7o864cq|class DataResource}
 */
const metricFilters = metricId => `/data/autocomplete/filters/metric/${metricId}`;

/**
 * GET a list of dimensions by metric.
 * @param {Numer} metricId
 * @see {@link https://tinyurl.com/yca7j4hz|class DataResource}
 */
const metricDimensions = metricId => `/data/autocomplete/dimensions/metric/${metricId}`;

/**
 * GET | Validates whether the entered dashboard name exists in inGraphs
 * @param {String} dashboardName
 */
const dashboardByName = (dashboardName, fabricGroup) => `/autometrics/isIngraphDashboard/${dashboardName}?fabricGroup=${fabricGroup}`;

/**
 * GET all metrics that belong to a dataset
 * @param {String} dataSet
 * @see {@link https://tinyurl.com/yb34ubfd|class MetricConfigResource}
 */
const metricsByDataset = dataSet => `/thirdeye-admin/metric-config/metrics?dataset=${dataSet}`;

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
 * General self-serve endpoints
 * TODO: rename these API utils according to entity, not UI features (self-serve, rca)
 */
export const selfServeApiCommon = {
  alertById,
  allConfigGroups,
  allApplications,
  alertFunctionByName,
  configGroupByAlertId,
  alertFunctionByAppName,
  metricAutoComplete
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
  selfServeApiCommon,
  selfServeApiOnboard,
  selfServeApiGraph
};
