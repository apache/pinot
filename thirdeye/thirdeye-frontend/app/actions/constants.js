
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
  lix: 0.05,
  gcn: 0.1,
  anomaly: 0.15,
  holiday: 0.2,
  informed: 0
};

export {
  COMPARE_MODE_MAPPING,
  colors,
  eventColorMapping,
  eventWeightMapping
};
