import { later } from '@ember/runloop';
import { computed } from '@ember/object';
import Component from '@ember/component';

export default Component.extend({
  events: [],
  selectedTab: 'all',

  start: null,
  end: null,

  /**
   * Returns event based on the selected tab
   */
  filteredEvents: computed(
    'eventsInRange.@each.type',
    'selectedTab',
    function() {
      const events = this.get('eventsInRange');
      const selectedTab = this.get('selectedTab');

      if (selectedTab === 'all') { return events; }
      return events
        .filter(event => (event.eventType === selectedTab))
        .sortBy('score')
        .reverse();
    }
  ),

  /**
   * Returns events in a range
   */
  eventsInRange: computed(
    'events',
    'start',
    'end',
    function() {
      const events = this.get('events');
      const start = this.get('start');
      const end = this.get('end');

      if (!(start && end)) { return events; }

      return events.filter((event) => {
        const {
          displayStart,
          displayEnd } = event;

        return (displayStart <= end) && (displayEnd >= start);
      });
    }
  ),

  // Require to display a loader for long rendering
  didUpdateAttrs(...args) {
    later(() => {
      this._super(args);
    });
  },

  /**
   * Columns required by ember-models-table
   */
  columns: [
    {
      template: 'custom/checkbox',
      useFilter: false,
      mayBeHidden: false,
      className: 'events-table__column--checkbox'
    },
    {
      propertyName: 'displayLabel',
      template: 'custom/eventLabel',
      title: 'Event Name',
      className: 'events-table__column'
    },
    {
      propertyName: 'eventType',
      title: 'Type',
      filterWithSelect: true,
      sortFilterOptions: true,
      className: 'events-table__column--compact'
    },
    {
      propertyName: 'humanRelStart',
      title: 'Start',
      className: 'events-table__column--compact',
      sortedBy: 'relStart',
      disableFiltering: true
    },
    {
      propertyName: 'humanDuration',
      title: 'Duration',
      className: 'events-table__column--compact',
      sortedBy: 'duration',
      disableFiltering: true
    }
    // {
    //   propertyName: 'score',
    //   title: 'Score',
    //   disableFiltering: true,
    //   className: 'events-table__column--compact',
    //   sortDirection: 'desc'
    // }
  ],

  actions: {
    /**
     * Handles tab selection
     * @param {String} tab String of the new selected tab
     */
    onTabChange(tab) {
      const currentTab = this.get('selectedTab');

      if (currentTab !== tab) {
        this.set('selectedTab', tab);
      }
    },

    /**
     * Handles event selectiion
     * @param {Object} event The new event Object
     */
    onSelection(event) {
      this.attrs.onSelection(event.urn);
    }
  }
});

