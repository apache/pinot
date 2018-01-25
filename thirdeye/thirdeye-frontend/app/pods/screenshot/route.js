import Ember from 'ember';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import UnauthenticatedRouteMixin from 'ember-simple-auth/mixins/unauthenticated-route-mixin';

export default Ember.Route.extend(UnauthenticatedRouteMixin, {
  model(params) {
    const { anomalyId: id} = params;
    const url = `/anomalies/search/anomalyIds/1/1/1?anomalyIds=${id}&functionName=`;

    return fetch(url).then(checkStatus);
  }
});
