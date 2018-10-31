import BaseAdapter from './base';

export default BaseAdapter.extend({
  //for api call - /dashboard/summary/autoDimensionOrder?dataset=search_v3_additive&metric=sat_click&currentStart=1524643200000&currentEnd=1525248000000&baselineStart=1524038400000&baselineEnd=1524643200000&summarySize=20&oneSideError=false&depth=3&dimensions=country_code#
  namespace: '/dashboard/summary'
});
