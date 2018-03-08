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
const metricAutoComplete = metricName => `/data/autocomplete/metric?name=${metricName}`;

/**
 * GET autocomplete request for alert by name
 * @param {String} functionName: alert name
 * @see {@link https://tinyurl.com/ybelagey|class DataResource}
 */
const alertAutoComplete = functionName => `/data/autocomplete/functionByName?name=${functionName}`;

/**
 * GET a single function record by id
 * @param {String} id: alert function id
 * @see {@link https://tinyurl.com/y76fu5vp|class OnboardResource}
 */
const alertById = functionId => `/onboard/function/${functionId}`;

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
 * General self-serve endpoints
 */
export const selfServeApiCommon = {
  alertById,
  allConfigGroups,
  allApplications,
  alertAutoComplete,
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
  deleteAlert
};

export default {
  selfServeApiCommon,
  selfServeApiOnboard,
  selfServeApiGraph
};
