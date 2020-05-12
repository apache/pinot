import Component from "@ember/component";
import { computed } from '@ember/object';
import { deleteProps,
  checkStatus } from 'thirdeye-frontend/utils/utils';

export default Component.extend({
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

  actions: {
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
