import { debounce, later } from '@ember/runloop';
import Controller from '@ember/controller';
import moment from 'moment';

export default Controller.extend({
  queryParams: ['dimension'],
  dimension: 'All',

  tableIsLoading: false,

  showDetails: false,
  selectedTab: 'details',

  dimensionsStart: 0,
  dimensionsEnd: 0,
  displayStart: 0,
  displayEnd: 0,
  dateFormat: 'MMM D, YYYY hh:mm a',

  subchartStart: 0,
  subchartEnd: 0,

  actions: {
    // Sets new dimension start and end
    setNewDate({ start, end }) {
      const dimensionsStart = moment(start).valueOf();
      const dimensionsEnd = moment(end).valueOf();

      this.setProperties({
        dimensionsEnd,
        dimensionsStart
      });
    },

    /**
     * Setting loading state to false on component's didRender
     */
    onRendering() {
      this.set('tableIsLoading', false);
    },

    onToggle(showDetails) {
      this.setProperties({
        showDetails,
        tableIsLoading: true
      });
    },

    /**
     * Handles subchart date change (debounced)
     */
    setDateParams([start, end]) {
      this.set('tableIsLoading', true);
      debounce(this, this.get('actions.setNewDate'), { start, end }, 500);
    },

    /**
     * Handles Contribution Table Tab selection
     * @param {String} tab Name of selected Tab
     */
    onTabChange(tab) {
      const currentTab = this.get('selectedTab');
      if (currentTab !== tab) {
        later(() => {
          this.setProperties({
            selectedTab: tab
          });
        });
      }
    }
  }

});
