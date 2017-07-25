import anomaly from './anomaly';
import metrics from './metrics';
import events from './events';
import dimensions from './dimensions';

import primaryMetric from './primary-metric';
import { combineReducers } from 'redux';

export default combineReducers({
  anomaly,
  events,
  metrics,
  dimensions,
  primaryMetric
});

