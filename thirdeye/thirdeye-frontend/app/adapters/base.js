import DS from 'ember-data';
import { inject as service } from '@ember/service';

/**
 * @summary This base supports the standard advanced configurations needed for making the requests.
 */
export default DS.RESTAdapter.extend({
  shareDashboardApiService: service('services/api/share-dashboard'),
  headers: {
    'Accept': '*/*'
  },
  ajaxOptions() {
    let ajaxOptions = this._super(...arguments);

    //TODO: update this to use another hook - lohuynh
    if (ajaxOptions.data.shareId) {
      delete ajaxOptions.data.shareId;//remove from query as not needed for actual request
    }
    // when server returns an empty response, we'll make it valid by replacing with {}
    ajaxOptions.converters = {
      'text json': function(data) {
        return data === '' ? {} : JSON.parse(data);
      }
    };

    return ajaxOptions;
  },

  handleResponse(status, headers, payload, requestData) {
    // For creates we look into the header
    if (status === 201 || status === 204) {
      headers['content-type'] = 'application/json';
      const id = headers['x-restli-id'] || headers['X-RestLi-Id'];
      if (id) {
        payload = { id };
      }
    }
    let response = this._super(status, headers, payload, requestData);
    return response;
  },

  /**
   * @summary The urlForQuery (called with query) works like the `query` hook above. Both allow mutating the request url.
   */
  urlForQuery (query, modelName) {
    /* The switch allows for adding more model names that needs custom request url */
    switch(modelName) {
      case 'performance': {
        return `${this.get('namespace')}/${query.appName}`;
      }
      case 'dimensions': {
        return `${this.get('namespace')}`;
      }
      case 'shareDashboard': {
        return `${this.get('namespace')}/${query.key}`;
      }
      case 'share': {
        const shareId = query.shareId;
        return `${this.get('namespace')}/${shareId}`;
      }
      default: {
        return this._super(...arguments);
      }
    }
  },

  /**
   * @summary Builds a URL for a record.save() call when the record has been deleted locally.
   * @param {string} modelName
   * @param {DS.Snapshot} snapshot
   * @return {string} url
   */
  urlForCreateRecord (modelName/*, snapshot*/) {
    /* The switch allows for adding more model names that needs custom request url */
    switch(modelName) {
      case 'share': {
        const hashKey = this.get('shareDashboardApiService').getHashKey();
        return `${this.get('namespace')}/${hashKey}`;
      }
      default: {
        return this._super(...arguments);
      }
    }
  },

  urlForUpdateRecord: function(id, modelName/*, snapshot*/) {
    return this._buildURL(modelName, id);
  }
});
