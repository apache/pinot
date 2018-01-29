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
     * Handle navigation to edit route
     */
    onClickEdit() {
      this.setProperties({
        isOverViewModeActive: false,
        isEditModeActive: true
      });
      this.transitionToRoute('manage.alert.edit', this.get('id'));
    },

    /**
     * Navigate back to manage base route
     */
    onClickNavToOverview() {
      this.transitionToRoute('manage.alert', this.get('id'));
    }
  }
});
