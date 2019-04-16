import BaseAdapter from './base';

export default BaseAdapter.extend({
  //for api call - /detection/subscription-groups
  namespace: '/detection',
  pathForType(type) {
    // path customization is needed here since ember data will convert to camel case by default
    return type.dasherize();
  }
});
