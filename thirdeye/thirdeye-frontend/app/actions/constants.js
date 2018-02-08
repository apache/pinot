
// endpoint cutoff for long periods
const ROOTCAUSE_ANALYSIS_DURATION_MAX = 1209600000; // 14 days (in millis)
const ROOTCAUSE_ANOMALY_DURATION_MAX = 604800000; // 7 days (in millis)

// compare mode to week offset mapping
const COMPARE_MODE_MAPPING = {
  WoW: 1,
  Wo2W: 2,
  Wo3W: 3,
  Wo4W: 4
};

// colors used to maintain consistent coloring throughout the app
const colors = [
  'orange',
  'teal',
  'purple',
  'red',
  'green',
  'pink'
];

// colors for events
const eventColorMapping = {
  holiday: 'green',
  informed: 'red',
  lix: 'purple',
  gcn: 'orange',
  anomaly: 'teal'
};

// colors for events
const baselineEventColorMapping = {
  holiday: 'light_green',
  informed: 'light_red',
  lix: 'light_purple',
  gcn: 'light_orange',
  anomaly: 'light_teal'
};

const eventWeightMapping = {
  informed: 0.025,
  lix: 0.075,
  anomaly: 0.225,
  gcn: 0.125,
  holiday: 0.175
};

export {
  COMPARE_MODE_MAPPING,
  colors,
  eventColorMapping,
  eventWeightMapping,
  baselineEventColorMapping,
  ROOTCAUSE_ANALYSIS_DURATION_MAX,
  ROOTCAUSE_ANOMALY_DURATION_MAX
};
