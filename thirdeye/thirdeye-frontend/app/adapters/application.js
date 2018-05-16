import BaseAdapter from './base';

export default BaseAdapter.extend({
  //for api call - /thirdeye/entity/APPLICATION
  namespace: '/thirdeye/entity',
  pathForType(type) {
    // path customization is needed here since ember data will try to plurize model names (applications)
    return type.toUpperCase();
  }
  //use shouldBackgroundReloadAll, shouldReloadAll for `findAll` which will pull from local + network each time
});
