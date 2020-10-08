/**
 * Handles metrics import from prseto dashboards
 * @module self-serve/create/import-metric
 * @exports import-metric
 */
import fetch from 'fetch';
import Controller from '@ember/controller';

export default Controller.extend({
  /**
   * Import Defaults
   */
  selectedDatabase: '',
  timeColumn: '',
  selectedTimeFormat: '',
  selectedTimeGranularity: '',
  selectedTimezone: 'UTC',
  response: '',

  init() {
    this._super(...arguments);
    this.aggregationOptions = ['SUM', 'AVG', 'COUNT', 'MAX' ];
    this.timeFormatOptions = ['EPOCH', 'yyyyMMdd', 'yyyy-MM-dd', 'yyyy-MM-dd-HH', 'yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd HH:mm:ss.S', 'yyyyMMddHHmmss', 'yyyy-MM-dd HH:mm:ss.SSS'];
    this.timeGranularityOptions = ['1MILLISECONDS', '1SECONDS', '1MINUTES', '1HOURS', '1DAYS', '1WEEKs', '1MONTHS', '1YEARS'];
    this.timezoneOptions = ["UTC", "Pacific/Midway", "US/Hawaii", "US/Alaska", "US/Pacific", "US/Arizona", "US/Mountain", "US/Central",
      "US/Eastern", "America/Caracas", "America/Manaus", "America/Santiago", "Canada/Newfoundland", "Brazil/East", "America/Buenos_Aires",
      "America/Godthab", "America/Montevideo", "Atlantic/South_Georgia", "Atlantic/Azores", "Atlantic/Cape_Verde", "Africa/Casablanca",
      "Europe/London", "Europe/Berlin", "Europe/Belgrade", "Europe/Brussels", "Europe/Warsaw", "Africa/Algiers", "Asia/Amman", "Europe/Athens",
      "Asia/Beirut", "Africa/Cairo", "Africa/Harare", "Europe/Helsinki", "Asia/Jerusalem", "Europe/Minsk", "Africa/Windhoek",
      "Asia/Baghdad", "Asia/Kuwait", "Europe/Moscow", "Africa/Nairobi", "Asia/Tbilisi", "Asia/Tehran", "Asia/Muscat", "Asia/Baku",
      "Asia/Yerevan", "Asia/Kabul", "Asia/Yekaterinburg", "Asia/Karachi", "Asia/Calcutta", "Asia/Colombo", "Asia/Katmandu",
      "Asia/Novosibirsk", "Asia/Dhaka", "Asia/Rangoon", "Asia/Bangkok", "Asia/Krasnoyarsk", "Asia/Hong_Kong", "Asia/Irkutsk",
      "Asia/Kuala_Lumpur", "Australia/Perth", "Asia/Taipei", "Asia/Tokyo", "Asia/Seoul", "Asia/Yakutsk", "Australia/Adelaide",
      "Australia/Darwin", "Australia/Brisbane", "Australia/Sydney", "Pacific/Guam", "Australia/Hobart", "Asia/Vladivostok", "Asia/Magadan",
      "Pacific/Auckland", "Pacific/Fiji", "Pacific/Tongatapu"];
    this.dimensions = [];
    this.metrics = [];
    this.databaseOptions = [];

    const databaseUrl = '/sql-data-source/databases';
    fetch(databaseUrl).then((res) => {
      var result = res.json();
      result.then((ret) => {
        for (var db in ret) {
          for (var table in ret[db]) {
            this.get('databaseOptions').pushObject(db + '.' + table + '.' + ret[db][table]);
          }
        }
      });
    }).catch(err => {});
  },

  /**
   * Defined actions for component
   */
  actions: {
    /**
     * Clears the validation error as user begins to type in field
     * @method clearExistingDashboardNameError
     * @return {undefined}
     */
    clearExistingDashboardNameError() {
      if (this.get('isDashboardExistError')) {
        this.set('isDashboardExistError', false);
      }
    },

    /**
     * Add a new dimension entry
     * @method addDimension
     * @return {undefined}
     */
    addDimension() {
      this.get('dimensions').pushObject({name: ''});
    },

    /**
     * Add a new metric entry
     * @method addMetric
     * @return {undefined}
     */
    addMetric() {
      var metricObj = {
        name: '',
        aggregationMethod: ''
      };
      this.get('metrics').pushObject(metricObj);
    },

    /**
     * Handles form submit
     * @method onSubmit
     * @return {undefined}
     */
    onSubmit() {
      const tableName = this.get('selectedDatabase').split('.').slice(0, 2).join('.') + '.' + this.get('tableName');
      const timeFormat = this.get('selectedTimeFormat');
      const timeGranularity = this.get('selectedTimeGranularity');
      const timeColumn = this.get('timeColumn');
      const dimensions = this.get('dimensions');
      const timezone = this.get('selectedTimezone');
      const dimensionArray = [];
      for (var i = 0; i < dimensions.length; i++) {
        dimensionArray.push(dimensions[i].name);
      }
      const metrics = this.get('metrics');
      const metricsMap = {};
      for (var j = 0; j < metrics.length; j++) {
        metricsMap[metrics[j].name] = metrics[j].aggregationMethod;
      }

      const importObj = {
        tableName: tableName,
        timeColumn: timeColumn,
        timeFormat: timeFormat,
        timezone: timezone,
        granularity: timeGranularity,
        dimensions: dimensionArray,
        metrics: metricsMap
      };

      const postProps = {
        method: 'post',
        body: JSON.stringify(importObj),
        headers: { 'content-type': 'Application/Json' }
      };
      const url = '/sql-data-source/onboard';
      fetch(url, postProps).then((res) => {
        if (res.status == 200) {
          this.set('response', 'Import success! However, it does not validate the database for your entries.');
        } else if (res.status == 400) {
          this.set('response', 'Some entries are wrong/missing, please try again.');
        }
      });
    }
  }
});
