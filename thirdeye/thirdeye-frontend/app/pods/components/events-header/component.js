import { alias } from '@ember/object/computed';
import { computed } from '@ember/object';
import Component from '@ember/component';

export default Component.extend({
  events: [],
  onTabChange: null,

  // default active tab
  activeTab: 'all',


  /**
   * Holiday Events
   */
  holidays: computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'holiday');
    }
  ),

  /**
   * GCN events
   */
  gcn: computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'gcn');
    }
  ),

  /**
   * anomaly events
   */
  anomaly: computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'anomaly');
    }
  ),


  /**
   * Informed events
   */
  informed: computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'informed');
    }
  ),


  /**
   * Lix events
   */
  lix: computed(
    'events.@each',
    function() {
      return this.get('events')
        .filter(event => event.eventType === 'lix');
    }
  ),

  /**
   * Count of events
   */
  allCount: alias('events.length'),
  holidayCount: alias('holidays.length'),
  gcnCount: alias('gcn.length'),
  informedCount: alias('informed.length'),
  lixCount: alias('lix.length'),
  anomalyCount: alias('anomaly.length'),

  actions: {
    // Handles tab change on click
    onTabClick(tab) {
      this.attrs.onTabChange(tab);
    }
  }
});
