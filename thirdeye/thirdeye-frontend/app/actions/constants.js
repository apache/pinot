
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

const eventWeightMapping = {
  informed: 0.025,
  lix: 0.075,
  anomaly: 0.125,
  gcn: 0.175,
  holiday: 0.175
};

export {
  COMPARE_MODE_MAPPING,
  colors,
  eventColorMapping,
  eventWeightMapping
};
