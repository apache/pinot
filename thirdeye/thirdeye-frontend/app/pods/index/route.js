import Ember from 'ember';
import fetch from 'fetch';
import RSVP from 'rsvp';

export default Ember.Route.extend({

  model() {
    return RSVP.hash({
      allAlertsConfig: fetch('thirdeye/entity/ALERT_CONFIG').then(res => res.json())
    })
  },
});