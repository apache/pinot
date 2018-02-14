/**
 * Controller for Alert Landing and Details Page
 * @module manage/alert
 * @exports manage/alert
 */
import Controller from '@ember/controller';

export default Controller.extend({
  /**
   * Set up to receive prompt to trigger page mode change.
   * When replay id is received it indicates that we need to check replay status
   * before displaying alert function performance data.
   */
  queryParams: ['jobId', 'functionName'],
  jobId: null,
  functionName: null,

  actions: {

    /**
     * Placeholder for subscribe button click action
     */
    onClickAlertSubscribe() {
      // TODO: Set user as watcher for this alert when API ready
    },

    /**
     * Handle conditions for display of appropriate alert nav link (overview or edit)
     */
    setEditModeActive() {
      this.setProperties({
        isOverViewModeActive: false,
        isEditModeActive: true
      });
    },

    /**
     * Handle navigation to edit route
     */
    onClickEdit() {
      this.send('transitionToEditPage', this.get('id'));
    },

    /**
     * Navigate to Tune Page
     */
    onClickNavToTune() {
      this.send('transitionToTunePage', this.get('id'));
    },

    /**
     * Navigate to Alert Page
     */
    onClickNavToOverview() {
      this.send('transitionToAlertPage', this.get('id'));
    }
  }
});
