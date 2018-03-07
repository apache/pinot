/**
 * GET request for all alert subscription groups
 * @see {@link namepathOrURL|link text}
 */
const allConfigGroups = '/thirdeye/entity/ALERT_CONFIG';

/**
 * GET request for all alert application groups
 * @see {@link namepathOrURL|link text}
 */
const allApplications = '/thirdeye/entity/APPLICATION';

/**
 * GET request for job status (a sequence of events including create, replay, autotune)
 * Requires 'jobId' query param
 * @see {@link namepathOrURL|link text}
 */
const jobStatus = jobId => `/detection-onboard/get-status?jobId=${jobId}`;

/**
 * POST request to create alert container in DB
 * Requires 'name' query param
 * @param {String} newAlertName: Name of new alert function
 * @see {@link namepathOrURL|link text}
 */
const createAlertFunction = newAlertName => `/function-onboard/create-function?name=${newAlertName}`;

/**
 * POST request to update an existing alert function with properties payload
 * Requires 'payload' object in request body and 'jobName' in query param
 * @param {String} jobName: unique name of new job for alert setup task
 * @see {@link namepathOrURL|link text}
 */
const updateAlertFunction = jobName => `/detection-onboard/create-job?jobName=${jobName}`;

/**
 * DELETE request to remove an alert function from the DB
 * Requires query param 'id'
 * @param {String} functionId: id of alert to remove
 * @see {@link namepathOrURL|link text}
 */
const deleteAlertFunction = functionId => `/dashboard/anomaly-function?id=${functionId}`;

/**
 * General self-serve endpoints
 */
export const general = {
  allConfigGroups,
  allApplications
};

/**
 * Onboarding endpoints for self-serve
 */
export const onboard = {
  jobStatus,
  createAlertFunction,
  updateAlertFunction,
  deleteAlertFunction
};

export default {
  general,
  onboard
};
