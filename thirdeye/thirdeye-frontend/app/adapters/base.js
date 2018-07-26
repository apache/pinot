import DS from 'ember-data';

/*
 * @summary This base supports the standard advanced configurations needed for making the requests.
 */
export default DS.RESTAdapter.extend({
  headers: {
    'Accept': '*/*'
  },
  ajaxOptions() {
    let ajaxOptions = this._super(...arguments);
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

  // TODO: Will keep for reference - lohuynh
  // query: function query(store, type, query) {
  //   const url = `${this.get('namespace')}/${query.appName}`;
  //   delete query.appName;//remove from query as not needed for actual request
  //   return this.ajax(url, 'GET', { data: query });
  // },
  
  // The urlForQuery works like the `query` hook above. Both allow mutating the request url.
  urlForQuery (query, modelName) {
    /* The switch allows for adding more model names that needs custom request url */
    switch(modelName) {
      case 'performance':
        return `${this.get('namespace')}/${query.appName}`;
      case 'dimensions':
        return `${this.get('namespace')}`;
      default:
        return this._super(...arguments);
    }
  }
});
