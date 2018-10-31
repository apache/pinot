import BaseAdapter from './base';

/*
 * @description This model is unique as it does not match the query url.
 * @example /detection-job/eval/application/lss?
*/
export default BaseAdapter.extend({
  //for api call - /detection-job/eval/application/{someAppName}?start={2017-09-01T00:00:00Z}&end={2018-04-01T00:00:00Z}
  namespace: '/detection-job/eval/application'
});
