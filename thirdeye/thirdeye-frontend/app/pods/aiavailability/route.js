/**
 * Handles the 'aiavailability' route.
 * @module aiavailability/route
 * @exports aiavailability model
 */

import moment from 'moment';
import Route from '@ember/routing/route';
import {
  hash
} from 'rsvp';
import {
  buildDateEod
} from 'thirdeye-frontend/utils/utils';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import { getAiAvailability } from 'thirdeye-frontend/utils/anomaly';
import { inject as service } from '@ember/service';

/**
 * Time range-related constants
 */
const displayDateFormat = 'YYYY-MM-DD HH:mm';
const defaultDurationObj = {
  duration: '1w',
  startDate: buildDateEod(1, 'week').valueOf(),
  endDate: moment().startOf('day').valueOf()
};

/**
 * Setup for query param behavior
 */
const queryParamsConfig = {
  refreshModel: true,
  replace: true
};

const detectionConfigId = '116702779'; // detectionConfigId for getting table data

export default Route.extend({
  session: service(),
  queryParams: {
    duration: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig
  },

  beforeModel(transition) {
    const { duration, startDate } = transition.queryParams;
    // Default to 1 week of anomalies to show if no dates present in query params
    if (!duration || !startDate) {
      this.transitionTo({ queryParams: defaultDurationObj });
    }
  },

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model(transition) {
    // Get duration data
    const {
      duration,
      startDate,
      endDate
    } = transition;

    return hash({
      // Fetch table data
      tableData: getAiAvailability(detectionConfigId, startDate, endDate),
      duration,
      startDate,
      endDate
    });
  },

  afterModel(model) {
    this._super(model);

    let tableData = (Array.isArray(model.tableData) && model.tableData.length > 0) ? model.tableData : [{ 'No Data Returned' : 'N/A'}];
    let tableHeaders = [...Object.keys(tableData[0]), 'link to RCA'];
    tableHeaders = tableHeaders.map(header => {
      return {
        text: header,
        sort: 'down'
      };
    });
    // augment response for direct mapping of keys to headers and fields to table cell
    const newTableData = [];
    tableData.forEach(flow => {
      const anomaliesArray = Object.keys(flow.comment);
      if (anomaliesArray.length > 0) {
        anomaliesArray.forEach(anomalyId => {
          const row = flow;
          row['link to RCA'] = anomalyId;
          row.comment = row.comment.anomalyId;
          // build array of strings to put on DOM
          row.columns = [];
          tableHeaders.forEach(header => {
            const cellValue = row[header.text];
            if (header.text === 'link to RCA') {
              const domString = `<a href="https://thirdeye.corp.linkedin.com/app/#/rootcause?anomalyId=${cellValue}">Comment of the anomaly</a>`;
              row.columns.push(domString);
            } else if (header.text === 'url') {
              const domString = `<a href="${cellValue}">Flow Link</a>`;
              row.columns.push(domString);
            } else if (!isNaN(parseFloat(cellValue)) && header.text != 'sla') {
              if (parseFloat(cellValue) < 0.99) {
                row.columns.push('<font color="red">'.concat(cellValue, '</font>'));
              } else {
                row.columns.push(cellValue);
              }
            } else {
              row.columns.push(cellValue);
            }
          });
          newTableData.push(row);
        });
      } else {
        const row = flow;
        row['link to RCA'] = null;
        row.comment = null;
        // build array of strings to put on DOM
        row.columns = [];
        tableHeaders.forEach(header => {
          const cellValue = row[header.text];
          if (header.text === 'link to RCA') {
            const domString = 'N/A';
            row.columns.push(domString);
          } else if (header.text === 'url') {
            const domString = `<a href="${cellValue}">Flow Link</a>`;
            row.columns.push(domString);
          } else if (!isNaN(parseFloat(cellValue)) && header.text != 'sla') {
            if (parseFloat(cellValue) < 0.99) {
              row.columns.push('<font color="red">'.concat(cellValue, '</font>'));
            } else {
              row.columns.push(cellValue);
            }
          }  else {
            row.columns.push(cellValue);
          }
        });
        newTableData.push(row);
      }
    });
    Object.assign(model, { tableData: newTableData, tableHeaders });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      startDate,
      endDate,
      duration,
      tableData,
      tableHeaders
    } = model;


    // Display loading banner
    controller.setProperties({
      timeRangeOptions: setUpTimeRangeOptions(['1w', '1m'], duration),
      activeRangeStart: moment(Number(startDate)).format(displayDateFormat),
      activeRangeEnd: moment(Number(endDate)).format(displayDateFormat),
      uiDateFormat: "MMM D, YYYY hh:mm a",
      timePickerIncrement: 5,
      // isDataLoading: true,
      tableData,
      tableHeaders
    });
  },

  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }
    },
    
    error() {
      return true;
    },

    /**
    * Refresh route's model.
    * @method refreshModel
    * @return {undefined}
    */
    refreshModel() {
      this.refresh();
    }
  }
});
