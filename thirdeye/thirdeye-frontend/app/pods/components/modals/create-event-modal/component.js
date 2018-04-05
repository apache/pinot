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
import {inject as service} from '@ember/service';

import fetch from 'fetch';

import createEventApi from 'thirdeye-frontend/utils/api/create-event';

export default Component.extend({

  session: service(), /**
   * Default selected mapping type
   * @type {String}
   */
  actions: {
    /**
     * Handles the close event
     * @return {undefined}
     */
    onExit() {
      this.set('isShowingModal', false);
    },

    onSave() {
      const {
        startTime, endTime, eventName
      } = getProperties(this, 'startTime', 'endTime', 'eventName');

      const startTimeSinceEpoch = new Date(startTime).getTime();
      const endTimeSinceEpoch = new Date(endTime).getTime();

      fetch(createEventApi.createEventUrl(startTimeSinceEpoch, endTimeSinceEpoch, eventName), {method: 'POST'})
    }
  }
});
