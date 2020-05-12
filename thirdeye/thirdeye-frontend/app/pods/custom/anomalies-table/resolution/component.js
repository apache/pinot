/**
 * Custom model table component
 * Constructs the select box for the resolution
 * @module custom/anomalies-table/resolution
 *
 * @example for usage in models table columns definitions
 *   {
 *     propertyName: 'feedback',
 *     component: 'custom/anomalies-table/resolution',
 *     title: 'Resolution',
 *     className: 'anomalies-table__column',
 *     disableFiltering: true
 *   },
 */
import Component from "@ember/component";
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';
import {
  computed,
  set,
  get,
  setProperties,
  getWithDefault
} from '@ember/object';
import { deleteProps,
  checkStatus } from 'thirdeye-frontend/utils/utils';

export default Component.extend({
  tagName: '', //using tagless so i can add my own in hbs
  anomalyResponseNames: anomalyUtil.anomalyResponseObj.mapBy('name'),
  anomalyDataUrl: getAnomalyDataUrl(),
  showResponseSaved: false,
  isUserReported: false,
  hasComment: false,
  renderStatusIcon: true,
  isDeleteSuccess: false,
  isDeleteFailure: false,
  openDeleteModal: false,

  headerText: computed(
    'record.id',
    function() {
      const record = this.get('record');
      return `Delete Anomaly #${record.get('id')}?`;
    }
  ),

  /**
   * Send a DELETE request to the report anomaly API (2-step process)
   * @method deleteAnomaly
   * @param {String} id - The anomaly id
   * @return {Promise}
   */
  _deleteAnomaly(id) {
    const reportUrl = `/detection/report-anomaly/${id}`;
    return fetch(reportUrl, deleteProps())
      .then((res) => checkStatus(res, 'delete'));
  },

  /**
   * Modal opener for "delete reported anomaly".
   * @method _triggerOpenDeleteModal
   * @return {undefined}
   */
  _triggerOpenDeleteModal() {
    this.setProperties({
      isDeleteSuccess: false,
      isDeleteFailure: false,
      openDeleteModal: true
    });
  },

  didReceiveAttrs() {
    this._super(...arguments);
    const anomalyComment = get(this.record.anomaly, 'comment');
    const hasComment = (anomalyComment && anomalyComment.replace(/ /g, '') !== 'null');
    const isUserReported = get(this.record.anomaly, 'source') === 'USER_LABELED_ANOMALY';
    setProperties(this, {
      hasComment,
      isUserReported
    });
  },

  actions: {
    /**
     * Handle dynamically saving anomaly feedback responses
     * @method onChangeAnomalyResponse
     * @param {Object} humanizedAnomaly - the humanized anomaly entity
     * @param {String} selectedResponse - user-selected anomaly feedback option
     * @param {Object} inputObj - the selection object
     */
    onChangeAnomalyResponse: async function(humanizedAnomaly, selectedResponse, inputObj) {
      const responseObj = anomalyUtil.anomalyResponseObj.find(res => res.name === selectedResponse);
      set(inputObj, 'selected', selectedResponse);
      // Reset icon display props
      setProperties(this, {
        renderStatusIcon: false,
        showResponseFailed: false,
        showResponseSaved: false
      });
      try {
        const id = humanizedAnomaly.get('id');
        // Save anomaly feedback
        await anomalyUtil.updateAnomalyFeedback(id, responseObj.value);
        // We make a call to ensure our new response got saved
        const savedAnomaly = await anomalyUtil.verifyAnomalyFeedback(id);
        // TODO: right now we will update the union wrapper cached record for this anomaly
        humanizedAnomaly.set('anomaly.feedback', responseObj.value);
        const filterMap = getWithDefault(savedAnomaly, 'feedback.feedbackType', null);
        // This verifies that the status change got saved as key in the anomaly statusFilterMap property
        const keyPresent = filterMap && (filterMap === responseObj.value);
        if (keyPresent) {
          humanizedAnomaly.set('anomalyFeedback', selectedResponse);
          set(this, 'showResponseSaved', true);
        } else {
          throw 'Response not saved';
        }
      } catch (err) {
        setProperties(this, {
          showResponseFailed: true,
          showResponseSaved: false
        });
      }
      set(this, 'renderStatusIcon', true);
    },

    /**
     * Handle delete anomaly modal cancel
     */
    onCancelDelete() {
      // If modal is still open after deleting, we don't want to keep calling REST api anymore
      if (!this.get('isDeleteFailure') && !this.get('isDeleteSuccess')) {
        this.setProperties({
          isDeleteSuccess: false,
          isDeleteFailure: false,
          openDeleteModal: false
        });
      }
    },

    /**
     * Handle submission of delete anomaly modal
     */
    onDelete() {
      // If modal is still open after deleting, we don't want to keep calling REST api anymore
      if (!this.get('isDeleteFailure') && !this.get('isDeleteSuccess')) {
        const record = this.get('record');
        this._deleteAnomaly(record.get('id'))
        // modal will stay open and inform user whether anomaly deleted.
          .then(() => {
            this.setProperties({
              isDeleteSuccess: true,
              isDeleteFailure: false
            });
          })
          .catch(() => {
            this.setProperties({
              isDeleteFailure: true,
              isDeleteSuccess: false
            });
          });
      }
    },

    /**
     * Open modal for deleting anomalies
     */
    onClickDeleteAnomaly(anomalyId) {
      this._triggerOpenDeleteModal(anomalyId);
    }
  }
});
