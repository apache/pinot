/**
 * Controller for Alert Landing and Details Page
 * @module manage/alert
 * @exports manage/alert
 */
import Ember from 'ember';

export default Ember.Controller.extend({
  /**
   * Set up to receive prompt to trigger page mode change.
   * When replay is received as true, it indicates that this is a
   * newly created alert and replay is needed in order to display anomaly data.
   */
  queryParams: ['replay', 'replayId'],
  replay: false,
  replayId: null,

  /**
   * Actions for alert page
   */
  actions: {

    /**
     * Placeholder for subscribe button click action
     * @method onClickAlertSubscribe
     * @return {undefined}
     */
    onClickAlertSubscribe() {
      // TODO: Set user as watcher for this alert when API ready
    }

  }
});
