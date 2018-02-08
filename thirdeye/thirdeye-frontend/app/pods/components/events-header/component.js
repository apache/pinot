import Ember from 'ember';

export default Ember.Component.extend({
  events: [],
  onTabChange: null,

  // default active tab
  activeTab: 'all',


  /**
   * Holiday Events
   */
  holidays: Ember.computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'holiday');
    }
  ),

  /**
   * GCN events
   */
  gcn: Ember.computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'gcn');
    }
  ),

  /**
   * anomaly events
   */
  anomaly: Ember.computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'anomaly');
    }
  ),


  /**
   * Informed events
   */
  informed: Ember.computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'informed');
    }
  ),


  /**
   * Lix events
   */
  lix: Ember.computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'lix');
    }
  ),

  /**
   * Count of events
   */
  allCount: Ember.computed.alias('events.length'),
  holidayCount: Ember.computed.alias('holidays.length'),
  gcnCount: Ember.computed.alias('gcn.length'),
  informedCount: Ember.computed.alias('informed.length'),
  lixCount: Ember.computed.alias('lix.length'),
  anomalyCount: Ember.computed.alias('anomaly.length'),

  actions: {
    // Handles tab change on click
    onTabClick(tab) {
      this.attrs.onTabChange(tab);
    }
  }
});
