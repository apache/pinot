import Ember from 'ember';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';
import fetch from 'fetch';

export default Ember.Route.extend({
  model(params) {
    const { anomalyId: id} = params;
    const url = `/anomalies/search/anomalyIds/1/1/1?anomalyIds=${id}&functionName=`;

    return fetch(url).then(checkStatus);
  }
});
