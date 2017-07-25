import Ember from 'ember';

export default Ember.Component.extend({
  events: [],
  selectedTab: 'all',

  start: null,
  end: null,

  /**
   * Returns event based on the selected tab
   */
  filteredEvents: Ember.computed(
    'eventsInRange.@each.type',
    'selectedTab',
    function() {
      const events = this.get('eventsInRange');
      const selectedTab = this.get('selectedTab');

      if (selectedTab === 'all') { return events; }
      return events
        .filter(event => (event.eventType === selectedTab))
        .sortBy('score');
    }
  ),

  /**
   * Returns events in a range
   */
  eventsInRange: Ember.computed(
    'events',
    'start',
    'end',
    function() {
      const events = this.get('events');
      const start = this.get('start');
      const end = this.get('end');

      if (!(start && end)) { return events; }

      return events.filter((event) => {
        return (event.end && ((event.end > start) && (event.end < end)))
          || ((event.start < end) && (event.start > start));
      });
    }
  ),

  // Require to display a loader for long rendering
  didUpdateAttrs(...args) {
    Ember.run.later(() => {
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
      className: 'events-table__column--flush'
    },
    {
      propertyName: 'label',
      title: 'Event Name',
      className: 'events-table__column'
    },
    {
      propertyName: 'eventType',
      title: 'Type',
      filterWithSelect: true,
      sortFilterOptions: true,
      className: 'events-table__column'
    },
    {
      template: 'custom/date-cell',
      title: 'Start Date - End Date',
      className: 'events-table__column'
    }
    // {
    //   propertyName: 'score',
    //   title: 'Score',
    //   disableFiltering: true,
    //   className: 'events-table__column',
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

