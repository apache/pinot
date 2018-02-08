/**
 * Mocks self-service alert onboarding responses
 */
export const onboardJobStatus = {
  "jobId": 3,
  "jobName": "smcclung_test_random-metric01:3142875X1",
  "jobStatus": "COMPLETED",
  "message": "",
  "taskStatuses": [
    {
      "taskName": "DataPreparation",
      "taskStatus": "COMPLETED",
      "message": ""
    },
    {
      "taskName": "FunctionAlertCreation",
      "taskStatus": "COMPLETED",
      "message": ""
    },
    {
      "taskName": "FunctionReplay",
      "taskStatus": "COMPLETED",
      "message": ""
    },
    {
      "taskName": "AlertFilterAutotune",
      "taskStatus": "COMPLETED",
      "message": ""
    },
    {
      "taskName": "Notification",
      "taskStatus": "COMPLETED",
      "message": ""
    }
  ]
};

export const onboardJobCreate = {
  "jobId": 3,
  "jobName": "smcclung_test_random-metric01:3142875X1",
  "jobStatus": "SCHEDULED",
  "message": "",
  "taskStatuses": []
};

export default {
  onboardJobStatus,
  onboardJobCreate
};
