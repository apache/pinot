import DS from 'ember-data';
import { inject as service } from '@ember/service';

const S_401_UNAUTHORIZED = 401;

const REST_HEADER = { Accept: '*/*' };

/**
 * @summary This base supports the standard advanced configurations needed for making the requests.
 */
export default DS.RESTAdapter.extend({
  session: service(),
  shareDashboardApiService: service('services/api/share-dashboard'),
  headers: REST_HEADER,
  ajaxOptions() {
    let ajaxOptions = this._super(...arguments);

    //TODO: update this to use another hook - lohuynh
    if (ajaxOptions.data.shareId) {
      delete ajaxOptions.data.shareId; //remove from query as not needed for actual request
    }
    // when server returns an empty response, we'll make it valid by replacing with {}
    ajaxOptions.converters = {
      'text json': function (data) {
        return data === '' ? {} : JSON.parse(data);
      }
    };

    return ajaxOptions;
  },

  handleResponse(status, headers, payload, requestData) {
    // For creates we look into the header
    if (status === S_401_UNAUTHORIZED) {
      this.get('session').invalidate();
    } else if (status === 201 || status === 204) {
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
   * @summary Handle any specific errors on request attempts
   */
  handleError(status) {
    if (status) {
      // Redirect if one of the errors is a 401 from the API.
      const unauthorized = status === S_401_UNAUTHORIZED;
      if (unauthorized) {
        this.get('session').invalidate();
      }
    }
  },

  /**
   * @summary The urlForQuery (called with query) works like the `query` hook above. Both allow mutating the request url.
   */
  urlForQuery(query, modelName) {
    /* The switch allows for adding more model names that needs custom request url */
    switch (modelName) {
      case 'performance': {
        return `${this.get('namespace')}/${query.appName}`;
      }
      case 'dimensions': {
        return `${this.get('namespace')}/${query.orderType}DimensionOrder`;
      }
      case 'shareDashboard': {
        return `${this.get('namespace')}/${query.key}`;
      }
      case 'share': {
        return `${this.get('namespace')}/${query.shareId}`;
      }
      case 'share-config': {
        return `${this.get('namespace')}/${query.appName}`;
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
  urlForCreateRecord(modelName /*, snapshot*/) {
    /* The switch allows for adding more model names that needs custom request url */
    switch (modelName) {
      case 'share': {
        const hashKey = this.get('shareDashboardApiService').getHashKey();
        return `${this.get('namespace')}/${hashKey}`;
      }
      default: {
        return this._super(...arguments);
      }
    }
  },

  /**
   * @summary Defining buildURL depending on what properties have changed
   * In this method, you need to examine the snapshot and adjust the URL accordingly.
   */
  urlForUpdateRecord: function (id, modelName /*, snapshot*/) {
    return this._buildURL(modelName, id);
  }
});
