import Ember from 'ember';
import fetch from 'fetch';
import RSVP from 'rsvp';

export default Ember.Route.extend({
  model() {
    const dataUrl = 'http://apucher-ld1:4200/timeseries/query?metricIds=17606337,17606338&ranges=1508454000000:1508543940000&granularity=5_MINUTES&transformations=timestamp';

    return RSVP.hash({
      data: fetch(dataUrl).then(res => res.json())
    });
  }
});
