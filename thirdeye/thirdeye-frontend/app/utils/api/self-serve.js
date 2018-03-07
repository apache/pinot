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
 * General self-serve endpoints
 */
export const selfServeApiCommon = {
  allConfigGroups,
  allApplications
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
  selfServeApiOnboard
};
