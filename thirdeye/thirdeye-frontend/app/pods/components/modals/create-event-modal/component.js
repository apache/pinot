/**
 * Component displaying the event creation inside a modal
 * @module components/modals/create-event-modal
 * @property {Function} onSave          - save the events
 * @property {Function} onExit          - toggle off the modal
 * @example
 {{modals/create-event-modal
   showCreateEventModal=showCreateEventModal
 }}
 * @exports create-event-modal
 * @author jihzhang
 */

import Component from '@ember/component';
import {
  get, set, getProperties
} from '@ember/object';
import { inject as service } from '@ember/service';

import fetch from 'fetch';

import createEventApi from 'thirdeye-frontend/utils/api/create-event';

export default Component.extend({

  session: service(),
  /**
   * Custom classes to be applied
   */
  classes: Object.freeze({
    theadCell: "te-modal__table-header"
  }),

  actions: {
    /**
     * Handles the close event
     * @return {undefined}
     */
    onExit() {
      this.set('isShowingModal', false);
    },

    /**
     * Handles save event
     * @return {undefined}
     */
    onSave() {
      const {
        startTime, endTime, eventName, countryCode
      } = getProperties(this, 'startTime', 'endTime', 'eventName', 'countryCode');

      const startTimeSinceEpoch = new Date(startTime).getTime();
      const endTimeSinceEpoch = new Date(endTime).getTime();

      fetch(createEventApi.createEventUrl(startTimeSinceEpoch, endTimeSinceEpoch, eventName, countryCode), {method: 'POST'})
    }
  }
});
