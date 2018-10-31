function InvestigateModel() {
  this.anomalyId;
  this.startDate = moment().subtract(1, 'days').startOf('day');
  this.endDate = moment().subtract(0, 'days').startOf('day');
  this.pageNumber = 1;
  this.functionName = '';
  this.renderViewEvent = new Event();
}

InvestigateModel.prototype = {
  init({anomalyId}) {
    if (this.anomalyId != anomalyId) {
      this.anomalyId = anomalyId;
      this.fetchMetricInformation(anomalyId);
    }
  },

  update(anomaly) {
    this.anomaly = anomaly;
    this.metricId = anomaly.metricId;
  },

  /**
   * Fetches information for a given anomalyId
   * @param  {Number} anomalyId Id of the anomaly to be fetched
   */
  fetchMetricInformation(anomalyId) {
    dataService.fetchAnomaliesForAnomalyIds(
          this.startDate, this.endDate, this.pageNumber, anomalyId, this.functionName, false, this.updateModelAndNotifyView.bind(this), 'investigate-spin-area');
  },

  getWowData() {
    return dataService.fetchAnomalyWowData(this.anomalyId);
  },

  getAnomaly() {
    return this.anomaly;
  },

  updateFeedback(userFeedback, comment) {
    const feedbackString = this.getFeedbackString(userFeedback);
    if (this.anomaly.anomalyFeedbackComments === comment
      && this.anomaly.anomalyFeedback === feedbackString) { return; }

    this.anomaly.anomalyFeedback = feedbackString;
    this.anomaly.anomalyFeedbackComments = comment;

    dataService.updateFeedback(this.anomalyId, userFeedback, comment);
  },

  getFeedbackString(userFeedback){
    return {
      [constants.FEEDBACK_TYPE_ANOMALY]: constants.FEEDBACK_STRING_CONFIRMED_ANOMALY,
      [constants.FEEDBACK_TYPE_NOT_ANOMALY]: constants.FEEDBACK_STRING_FALSE_ALARM,
      [constants.FEEDBACK_TYPE_ANOMALY_NEW_TREND]: constants.FEEDBACK_STRING_CONFIRMED_NEW_TREND
    }[userFeedback];
  },

  getFeedbackType(){
    return {
      [constants.FEEDBACK_STRING_CONFIRMED_ANOMALY]: constants.FEEDBACK_TYPE_ANOMALY ,
      [constants.FEEDBACK_STRING_FALSE_ALARM]: constants.FEEDBACK_TYPE_NOT_ANOMALY,
      [constants.FEEDBACK_STRING_CONFIRMED_NEW_TREND]: constants.FEEDBACK_TYPE_ANOMALY_NEW_TREND
    }[this.anomaly.anomalyFeedback];
  },

  /**
   * Call back function rendering the view
   * @param  {Object} args Result payload
   */
  updateModelAndNotifyView(args = {}) {
    const { anomalyDetailsList } = args;
    const [anomaly]  = anomalyDetailsList;

    this.update(anomaly);
    this.formatAnomaly();
    this.renderViewEvent.notify();
  },
/**
   * Helper Function that returns formatted anomaly region duration data for UI
   * @param  {date}   start       the anomaly region start
   * @param  {date}   end         the anomaly region end
   * @param  {string} granularity the granularity of the anomaly
   * @return {string}         formatted start - end date/time
   */
  getRegionDuration(start, end, granularity) {

    if (!(start && end)) {
      return 'N/A';
    }
    const regionStart = moment.tz(start, constants.TIMESERIES_DATE_FORMAT, constants.TIME_ZONE);
    const regionEnd = moment.tz(end, constants.TIMESERIES_DATE_FORMAT, constants.TIME_ZONE);
    const isSameDay = regionStart.isSame(regionEnd, 'day');
    const timeDelta = regionEnd.diff(regionStart);
    const regionDuration = moment.duration(timeDelta);
    const showTime = granularity !== 'DAYS';
    let range = '';
    let regionStartFormat = constants.DETAILS_DATE_FORMAT;
    let regionEndFormat = constants.DETAILS_DATE_FORMAT;

    if (showTime) {
      regionStartFormat += `, ${constants.DETAILS_DATE_HOURS_FORMAT}`;
      regionEndFormat += `, ${constants.DETAILS_DATE_HOURS_FORMAT}`;
    }

    if (isSameDay) {
      regionEndFormat = '';
    }

    if (isSameDay && showTime) {
      regionEndFormat = constants.DETAILS_DATE_HOURS_FORMAT;
    }

    return `${regionDuration.humanize()} (${regionStart.format(regionStartFormat)}${regionEndFormat ? ' - ' + regionEnd.format(regionEndFormat) : ''})`;
  },

  /**
   * Helper Function that retuns formatted change delta for UI
   * @param  {int}    current    current average anomaly
   * @param  {int}    baseline   baseline the anomaly is compared too
   * @return {string}            'N/A' if either is missing, otherwise formatted delta (%)
   */
  getChangeDelta(current, baseline) {
    let changeDelta = 'N/A';
    if (current && baseline) {
      const amount = (current - baseline) / baseline * 100;
      changeDelta = `${amount.toFixed(2)}%`;
    }

    return changeDelta;
  },

  /**
   * Helper Function that sets formatted duration and changeDelta onto the anomaly model
   * @return {null}
   */
  formatAnomaly() {
    const anomaly = this.anomaly;
    this.anomaly.duration = this.getRegionDuration(anomaly.anomalyStart, anomaly.anomalyEnd, anomaly.timeUnit);
    this.anomaly.changeDelta = this.getChangeDelta(anomaly.current, anomaly.baseline);
  },
};
