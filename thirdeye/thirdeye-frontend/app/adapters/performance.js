import BaseAdapter from './base';
/*
 * @description This model is unique as it does not match the query url.
 * @example /detection-job/eval/application/{appName}?appName=appName&end=2018-05-16T07%3A00%3A00Z&start=2018-05-09T07%3A00%3A00Z
 */
export default BaseAdapter.extend({
  //name space for api call - /detection-job/eval/application
  namespace: '/detection-job/eval/application'
});
