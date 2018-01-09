/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import fetch from 'fetch';
import moment from 'moment';
import Route from '@ember/routing/route';
import { checkStatus, postProps, buildDateEod, toIso } from 'thirdeye-frontend/helpers/utils';
import { enhanceAnomalies, setUpTimeRangeOptions, toIdGroups, evalObj } from 'thirdeye-frontend/helpers/manage-alert-utils';

/**
 * Basic alert page defaults
 */
const durationDefault = '3m';
const dateFormat = 'YYYY-MM-DD';
const startDateDefault = buildDateEod(3, 'month');
const endDateDefault = buildDateEod(1, 'day');

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
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {Ember.RSVP promise}
 */
const fetchCombinedAnomalies = (anomalyIds) => {
  let anomalyPromises = [];
  if (anomalyIds.length) {
    const idGroups = toIdGroups(anomalyIds);
    const anomalyPromiseHash = idGroups.map((group, index) => {
      let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
      let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`).then(checkStatus);
      return Ember.RSVP.resolve(getAnomalies);
    });
    anomalyPromises = Ember.RSVP.all(anomalyPromiseHash);
  }
  return anomalyPromises;
};

/**
 * Returns a promise hash to fetch to fetch fresh projected anomaly data after tuning adjustments
 * @param {Date} startDate - start of date range
 * @param {Date} endDate - end of date range
 * @param {Sting} tuneId - current autotune filter Id
 * @param {String} alertId - current alert Id
 * @returns {Object} containing fetch promises
 */
const tuningPromiseHash = (startDate, endDate, tuneId, alertId) => {
  const baseStart = moment(Number(startDate));
  const baseEnd = moment(Number(endDate));
  const tuneParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
  const qsParams = `start=${baseStart.utc().format(dateFormat)}&end=${baseEnd.utc().format(dateFormat)}&useNotified=true`;
  const projectedUrl = `/detection-job/eval/autotune/${tuneId}?${tuneParams}`;
  const projectedMttdUrl = `/detection-job/eval/projected/mttd/${tuneId}`;
  const anomaliesUrlA = `/dashboard/anomaly-function/${alertId}/anomalies?${qsParams}`;
  const anomaliesUrlB =`/detection-job/eval/projected/anomalies/${tuneId}?${qsParams}`;

  return {
    projectedMttd: fetch(projectedMttdUrl).then(checkStatus),
    projectedEval: fetch(projectedUrl).then(checkStatus), // NOTE: ensure API returns JSON
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

    // Default to 1 month of anomalies to show if no dates present in query params
    if (!duration || (duration !== 'custom' && duration !== '3m') || !startDate) {
      this.transitionTo({ queryParams: {
        duration: durationDefault,
        startDate: startDateDefault.valueOf(),
        endDate: endDateDefault.valueOf()
      }});
    }
  },

  model(params, transition) {
    const { id, alertData } = this.modelFor('manage.alert');
    if (!id) { return; }

    const {
      duration,
      startDate,
      endDate
    } = transition.queryParams;

    // Prepare endpoints for the initial eval, mttd, projected metrics calls
    const tuneParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const tuneIdUrl = `/detection-job/autotune/filter/${id}?${tuneParams}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${tuneParams}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}`;
    const initialPromiseHash = {
      evalData: fetch(evalUrl).then(checkStatus), // NOTE: ensure API returns JSON
      autotuneId: fetch(tuneIdUrl, postProps('')).then(checkStatus),
      mttd: fetch(mttdUrl).then(checkStatus)
    };

    return Ember.RSVP.hash(initialPromiseHash)
      .then((alertEvalMetrics) => {
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
      .catch((err) => {
        // TODO: Display default error banner in the event of fetch failure
      });
  },

  afterModel(model) {
    this._super(model);

    const {
      id: alertId,
      startDate,
      endDate,
      alertEvalMetrics
    } = model;

    return Ember.RSVP.hash(tuningPromiseHash(startDate, endDate, alertEvalMetrics.autotuneId, alertId))
      .then((data) => {
        const idsRemoved = anomalyDiff(data.idListA, data.idListB).idsRemoved;
        Object.assign(model.alertEvalMetrics, { projected: data.projectedEval });
        return fetchCombinedAnomalies(idsRemoved);
      })
      // Fetch all anomaly data for returned Ids to paginate all from one array
      .then((rawAnomalyData) => {
        Object.assign(model, { rawAnomalyData });
      })
      // Got errors?
      .catch((err) => {
        Object.assign(model, { loadError: true, loadErrorMsg: err });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
      alertData,
      duration,
      loadError,
      alertEvalMetrics,
      rawAnomalyData
    } = model;

    const anomalyData = enhanceAnomalies(rawAnomalyData);
    const tableStats = anomalyTableStats(anomalyData);
    const timeRangeOptions = setUpTimeRangeOptions([durationDefault], duration);

    // Prime the controller
    controller.setProperties({
      alertData,
      loadError,
      tableStats,
      alertId: id,
      timeRangeOptions,
      anomalyData,
      alertEvalMetrics
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
        startDate: startDateDefault.valueOf(),
        endDate: endDateDefault.valueOf()
      }});
    },

    // User clicks "save" on previewed tune settings
    submitTuningRequest(tuneId) {
      this.saveAutoTuneSettings(tuneId).then((result) => {
        this.controller.set('isTuneSaveSuccess', true);
      })
      .catch((error) => {
        this.controller.set('isTuneSaveFailure', true);
        this.controller.set('failureMessage', error);
      });
    },

    // User clicks "preview", having configured performance settings
    triggerTuningSequence(configString) {
      const {
        id: alertId,
        startDate,
        endDate,
        tuneIdUrl
      } = this.currentModel;
      let projectedStats = {};

      fetch(tuneIdUrl + configString, postProps('')).then(checkStatus)
        .then((autoTuneId) => {
          return Ember.RSVP.hash(tuningPromiseHash(startDate, endDate, autoTuneId[0], alertId));
        })
        .then((data) => {
          const idsRemoved = anomalyDiff(data.idListA, data.idListB).idsRemoved;
          projectedStats = data.projectedEval;
          projectedStats.mttd = data.projectedMttd;
          return fetchCombinedAnomalies(idsRemoved);
        })
        .then((rawAnomalyData) => {
          const anomalyData = enhanceAnomalies(rawAnomalyData);
          this.controller.set('alertEvalMetrics.projected', projectedStats);
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
