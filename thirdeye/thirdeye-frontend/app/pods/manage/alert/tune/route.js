/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import RSVP from "rsvp";
import _ from 'lodash';
import fetch from 'fetch';
import moment from 'moment';
import Route from '@ember/routing/route';
import { isPresent } from "@ember/utils";
import { later } from "@ember/runloop";
import {
  checkStatus,
  postProps,
  buildDateEod,
  toIso
} from 'thirdeye-frontend/utils/utils';
import {
  enhanceAnomalies,
  setUpTimeRangeOptions,
  toIdGroups,
  extractSeverity,
  getDuration
} from 'thirdeye-frontend/utils/manage-alert-utils';

/**
 * Basic alert page defaults
 */
const durationDefault = '3m';
const defaultSeverity = '0.3';
const dateFormat = 'YYYY-MM-DD';
const displayDateFormat = 'YYYY-MM-DD HH:mm';
const startDateDefault = buildDateEod(3, 'month').valueOf();
const endDateDefault = moment().valueOf();

/**
 * Pattern display options (power-select) and values
 */
const patternMap = {
  'Up and Down': 'UP,DOWN',
  'Up Only': 'UP',
  'Down Only': 'DOWN'
};

/**
 * Severity display options (power-select) and values
 */
const severityMap = {
  'Percentage of Change': 'weight',
  'Absolute Value of Change': 'deviation'
};

/**
 * If no filter data is set for sensitivity, use this
 */
const sensitivityDefaults = {
  selectedSeverityOption: 'Percentage of Change',
  selectedTunePattern: 'Up and Down',
  defaultPercentChange: '0.3',
  defaultMttdChange: '5'
};

/**
 * Build the object to populate anomaly table feedback categories
 * @param {Array} anomalies - list of all deduped and filtered anomalies
 * @returns {Object}
 */
const anomalyTableStats = (anomalies) => {
  const trueAnomalies = anomalies ? anomalies.filter(anomaly => anomaly.anomalyFeedback === 'True anomaly') : 0;
  const falseAnomalies = anomalies ? anomalies.filter(anomaly => anomaly.anomalyFeedback === 'False Alarm') : 0;
  const userAnomalies = anomalies ? anomalies.filter(anomaly => anomaly.anomalyFeedback === 'Confirmed - New Trend') : 0;

  return [
    {
      count: anomalies.length,
      label: 'All',
      isActive: true
    },
    {
      count: trueAnomalies.length,
      label: 'True Anomalies',
      isActive: false
    },
    {
      count: falseAnomalies.length,
      label: 'False Alarms',
      isActive: false
    },
    {
      count: userAnomalies.length,
      label: 'User Created',
      isActive: false
    }
  ];
};

/**
 * Set up select & input field defaults for sensitivity settings
 * @param {Array} alertData - properties for the currently loaded alert
 * @returns {Object}
 */
const processDefaultTuningParams = (alertData) => {
  let {
    selectedSeverityOption,
    selectedTunePattern,
    defaultPercentChange,
    defaultMttdChange
  } = sensitivityDefaults;

  // Cautiously derive tuning data from alert filter properties
  const featureString = 'window_size_in_hour';
  const alertFilterObj = alertData.alertFilter || null;
  const alertPattern = alertFilterObj ? alertFilterObj.pattern : null;
  const isFeaturesPropFormatted = _.has(alertFilterObj, 'features') && alertFilterObj.features.includes(featureString);
  const isMttdPropFormatted =  _.has(alertFilterObj, 'mttd') && alertFilterObj.mttd.includes(`${featureString}=`);
  const alertFeatures = isFeaturesPropFormatted ? alertFilterObj.features.split(',')[1] : null;
  const alertMttd = isMttdPropFormatted ? alertFilterObj.mttd.split(';') : null;

  // Load saved pattern into pattern options
  const savedTunePattern = alertPattern ? alertPattern : 'UP,DOWN';
  for (var patternKey in patternMap) {
    if (savedTunePattern === patternMap[patternKey]) {
      selectedTunePattern = patternKey;
    }
  }

  // TODO: enable once issue resolved in backend (not saving selection to new feature string)
  // savedSeverityPattern = alertFeatures ? alertFeatures : 'weight';
  const savedSeverityPattern = alertMttd ? alertMttd[1].split('=')[0] : 'weight';
  const isAbsValue = savedSeverityPattern === 'deviation';
  for (var severityKey in severityMap) {
    if (savedSeverityPattern === severityMap[severityKey]) {
      selectedSeverityOption = severityKey;
    }
  }

  // Load saved mttd
  const mttdValue = alertMttd ? alertMttd[0].split('=')[1] : 'N/A';
  const customMttdChange = !isNaN(mttdValue) ? Number(mttdValue).toFixed(2) : defaultMttdChange;

  // Load saved severity value
  const severityValue = alertMttd ? alertMttd[1].split('=')[1] : 'N/A';
  const rawPercentChange = !isNaN(severityValue) ? Number(severityValue) : defaultPercentChange;
  const customPercentChange = isAbsValue ? rawPercentChange : rawPercentChange * 100;

  return { selectedSeverityOption, selectedTunePattern, customPercentChange, customMttdChange };
};

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {RSVP promise}
 */
const fetchCombinedAnomalies = (anomalyIds) => {
  if (anomalyIds.length) {
    const idGroups = toIdGroups(anomalyIds);
    const anomalyPromiseHash = idGroups.map((group, index) => {
      let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
      let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`).then(checkStatus);
      return RSVP.resolve(getAnomalies);
    });
    return RSVP.all(anomalyPromiseHash);
  } else {
    return RSVP.resolve([]);
  }
};

/**
 * Fetches severity scores for all anomalies
 * TODO: Move this and other shared requests to a common service
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {RSVP promise}
 */
const fetchSeverityScores = (anomalyIds) => {
  if (anomalyIds.length) {
    const anomalyPromiseHash = anomalyIds.map((id) => {
      return RSVP.hash({
        id,
        score: fetch(`/dashboard/anomalies/score/${id}`).then(checkStatus)
      });
    });
    return RSVP.allSettled(anomalyPromiseHash);
  } else {
    return RSVP.resolve([]);
  }
};

/**
 * Returns a promise hash to fetch to fetch fresh projected anomaly data after tuning adjustments
 * @param {Date} startDate - start of date range
 * @param {Date} endDate - end of date range
 * @param {Sting} tuneId - current autotune filter Id
 * @param {String} alertId - current alert Id
 * @returns {Object} containing fetch promises
 */
const tuningPromiseHash = (startDate, endDate, tuneId, alertId, severity = defaultSeverity) => {
  const baseStart = moment(Number(startDate));
  const baseEnd = moment(Number(endDate));
  const tuneParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
  const qsParams = `start=${baseStart.utc().format(dateFormat)}&end=${baseEnd.utc().format(dateFormat)}&useNotified=true`;
  const projectedUrl = `/detection-job/eval/autotune/${tuneId}?${tuneParams}`;
  const projectedMttdUrl = `/detection-job/eval/projected/mttd/${tuneId}?severity=${severity}`;
  const anomaliesUrlA = `/dashboard/anomaly-function/${alertId}/anomalies?${qsParams}`;
  const anomaliesUrlB =`/detection-job/eval/projected/anomalies/${tuneId}?${qsParams}`;

  return {
    projectedMttd: fetch(projectedMttdUrl).then(checkStatus),
    projectedEval: fetch(projectedUrl).then(checkStatus),
    idListA: fetch(anomaliesUrlA).then(checkStatus),
    idListB: fetch(anomaliesUrlB).then(checkStatus)
  };
};

/**
 * Returns a bi-directional diff given "before" and "after" tuning anomaly Ids
 * @param {Array} listA - list of all anomaly ids BEFORE tuning
 * @param {Array} listB - list of all anomaly ids AFTER tuning
 * @returns {Object}
 */
const anomalyDiff = (listA, listB) => {
  return {
    idsRemoved: listA.filter(id => !listB.includes(id)),
    idsAdded: listB.filter(id => !listA.includes(id))
  };
};

/**
 * Setup for query param behavior
 */
const queryParamsConfig = {
  refreshModel: true,
  replace: true
};

export default Route.extend({
  queryParams: {
    duration: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig
  },

  beforeModel(transition) {
    const { duration, startDate } = transition.queryParams;

    // Default to 3 months of anomalies to show if no dates present in query params
    if (!duration || (duration !== 'custom' && duration !== '3m') || !startDate) {
      this.transitionTo({ queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault
      }});
    }
  },

  model(params, transition) {
    const { id, alertData } = this.modelFor('manage.alert');
    if (!id) { return; }
    // Fetch saved duration data
    const { duration, startDate, endDate } = getDuration();

    // Prepare endpoints for the initial eval, mttd, projected metrics calls
    const tuneParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const tuneIdUrl = `/detection-job/autotune/filter/${id}?${tuneParams}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${tuneParams}&isProjected=TRUE`;
    const mttdUrl = `/detection-job/eval/mttd/${id}?severity=${extractSeverity(alertData, defaultSeverity)}`;
    const initialPromiseHash = {
      current: fetch(evalUrl).then(checkStatus),
      mttd: fetch(mttdUrl).then(checkStatus)
    };

    return RSVP.hash(initialPromiseHash)
      .then((alertEvalMetrics) => {
        Object.assign(alertEvalMetrics.current, { mttd: alertEvalMetrics.mttd});
        return {
          id,
          alertData,
          duration,
          tuneIdUrl,
          startDate,
          endDate,
          tuneParams,
          alertEvalMetrics
        };
      })
      .catch((error) => {
        return RSVP.reject({ error, location: `${this.routeName}:model`, calls: initialPromiseHash });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
      alertData,
      duration,
      loadError,
      startDate,
      endDate,
      alertEvalMetrics
    } = model;

    // Conditionally add select option for severity
    if (alertData.toCalculateGlobalMetric) {
      severityMap['Site Wide Impact'] = 'site_wide_impact';
    }

    // Prepare sensitivity default values to populate tuning options from alert data
    const {
      selectedSeverityOption,
      selectedTunePattern,
      customPercentChange,
      customMttdChange
    } = processDefaultTuningParams(alertData);
    Object.assign(model, { customPercentChange, customMttdChange });

    controller.setProperties({
      alertData,
      loadError,
      patternMap,
      severityMap,
      alertId: id,
      autoTuneId: '',
      customMttdChange,
      customPercentChange,
      alertEvalMetrics,
      selectedTunePattern,
      selectedSeverityOption,
      alertHasDimensions: isPresent(alertData.exploreDimensions),
      timeRangeOptions: setUpTimeRangeOptions([durationDefault], duration)
    });
    controller.initialize();

    // Ensure date range picker gets populated correctly
    later(this, () => {
      controller.setProperties({
        activeRangeStart: moment(Number(startDate)).format(displayDateFormat),
        activeRangeEnd: moment(Number(endDate)).format(displayDateFormat)
      });
    });
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

  saveAutoTuneSettings(id) {
    return fetch(`/detection-job/update/filter/${id}`, postProps('')).then(checkStatus);
  },

  actions: {

    // User clicks reset button
    resetPage() {
      this.transitionTo({ queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault
      }});
    },

    // User resets settings
    resetTuningParams(alertData) {
      const {
        selectedSeverityOption,
        selectedTunePattern,
        customPercentChange,
        customMttdChange
      } = processDefaultTuningParams(alertData);
      this.controller.setProperties({
        selectedSeverityOption,
        selectedTunePattern,
        customPercentChange,
        customMttdChange
      });
    },

    // User clicks "save" on previewed tune settings
    submitTuningRequest(tuneId) {
      this.saveAutoTuneSettings(tuneId)
        .then((result) => {
          this.controller.set('isTuneSaveSuccess', true);
        })
        .catch((error) => {
          this.controller.set('isTuneSaveFailure', true);
          this.controller.set('failureMessage', error);
        });
    },

    // User clicks "preview", having configured performance settings
    triggerTuningSequence(configObj) {
      const { configString, severityVal} = configObj;
      const {
        id: alertId,
        startDate,
        endDate,
        tuneIdUrl
      } = this.currentModel;
      let projectedStats = {};
      let rawAnomalies = [];
      let idsRemoved = [];

      fetch(tuneIdUrl + configString, postProps('')).then(checkStatus)
        .then((tuneId) => {
          const autoTuneId = tuneId[0];
          this.controller.set('autoTuneId', autoTuneId);
          return RSVP.hash(tuningPromiseHash(startDate, endDate, autoTuneId, alertId, severityVal));
        })
        .then((data) => {
          idsRemoved = anomalyDiff(data.idListA, data.idListB).idsRemoved;
          projectedStats = data.projectedEval;
          projectedStats.mttd = data.projectedMttd;
          this.controller.set('alertEvalMetrics.projected', projectedStats);
          this.controller.setProperties({
            isTunePreviewActive: true,
            isPerformanceDataLoading: false
          });
          return fetchCombinedAnomalies(idsRemoved);
        })
        .then((rawAnomalyData) => {
          rawAnomalies = rawAnomalyData;
          return fetchSeverityScores(idsRemoved);
        })
        .then((severityScores) => {
          const anomalyData = enhanceAnomalies(rawAnomalies, severityScores);
          this.controller.setProperties({
            anomalyData,
            tableStats: anomalyTableStats(anomalyData)
          });
        })
        // Got errors?
        .catch((err) => {
          this.controller.setProperties({
            loadError: true,
            loadErrorMsg: err
          });
        });
    }
  }
});
