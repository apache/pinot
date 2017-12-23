/**
 * Mocks email subscription groups for alerts
 */
export default [
  {
    "id": 1,
    "version": 0,
    "createdBy": null,
    "updatedBy": null,
    "name": "test_alert_1",
    "application": "the-lion-king",
    "cronExpression": "test_cron_1",
    "active": true,
    "emailConfig": {
      "anomalyWatermark": 0,
      "functionIds": [1]
    },
    "reportConfigCollection": {
      "enabled": true,
      "intraDay": false,
      "delayOffsetMillis": 0,
      "reportMetricConfigs": [{
        "compareMode": "WoW",
        "metricId": 100000,
        "dimensions": ["countryCode", "minor_continent", "religion"]
      }],
      "contactEmail": "simba@disney.com"
    },
    "alertGroupConfig": null,
    "recipients": "simba@disney.com",
    "fromAddress": "mufasa@disney.com"
  }, {
    "id": 2,
    "version": 0,
    "createdBy": null,
    "updatedBy": null,
    "name": "test_alert_2",
    "application": "beauty-and-the-beast",
    "cronExpression": "test_cron_2",
    "active": true,
    "emailConfig": {
      "anomalyWatermark": 0,
      "functionIds": [2]
    },
    "reportConfigCollection": {
      "enabled": true,
      "intraDay": true,
      "delayOffsetMillis": 0,
      "reportMetricConfigs": [{
        "compareMode": "WoW",
        "metricId": 100001,
        "dimensions": ["countryCode"]
      }, {
        "compareMode": "WoW",
        "metricId": 100002,
        "dimensions": ["countryCode"]
      }],
      "contactEmail": "gaston@disney.com"
    },
    "alertGroupConfig": null,
    "recipients": "gaston@disney.com",
    "fromAddress": "belle@disney.com"
  }];
