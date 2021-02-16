import BaseAdapter from './base';
/*
 * @description This model is unique as it does not match the query url.
 * @example /config/shareDashboard/{mykey}
 */
export default BaseAdapter.extend({
  //for api call - /config/shareDashboard/mykey
  //curl '/config/shareDashboard/mykey' -d '{"key":["v1","v2"],"nested":{"otherkey":6789}}' -H 'Cookie: te_auth=fbdff8bd8a5a2f1d302331db5a3abc432a225f2ee2eba14e5a97a85f8272b1d5'
  namespace: '/config/shareDashboard'
});
