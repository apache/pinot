import BaseAdapter from './base';
import $ from 'jquery';

/*
 * @description This model is unique as the query is dynamic base on appName.
 * @example ${this.get('namespace')}/${query.appName};
 */
export default BaseAdapter.extend({
  namespace: '/config/shareDashboardTemplates',
  queryRecord(store, type, query) {
    return $.getJSON(`${this.get('namespace')}/${query.appName}`);
  }
});
