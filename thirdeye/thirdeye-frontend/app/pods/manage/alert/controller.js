/**
 * Controller for Alert Landing and Details Page
 * @module manage/alert
 * @exports manage/alert
 */
import Controller from '@ember/controller';
import { setProperties } from '@ember/object';

export default Controller.extend({
  /**
   * Set up to receive prompt to trigger page mode change.
   * When replay id is received it indicates that we need to check replay status
   * before displaying alert function performance data.
   */
  queryParams: ['jobId', 'functionName'],
  jobId: null,
  functionName: null,
  isOverviewLoaded: true,

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
      this.set('isEditModeActive', true);
    },

    /**
     * Handle navigation to edit route
     */
    onClickEdit() {
      this.set('isEditModeActive', true);
      this.transitionToRoute('manage.alert.edit', this.get('id'), { queryParams: { refresh: true }});
    },

    /**
     * Navigate to Alert Page
     */
    onClickNavToOverview() {
      this.set('isEditModeActive', false);
      this.transitionToRoute('manage.alert.explore', this.get('id'));
    }
  }
});
