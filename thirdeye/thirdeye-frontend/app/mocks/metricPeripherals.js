/**
 * Mock for filters, dimensions and other metric-associated datasets used in alert creation/edit
 */
export const filters = {
  "container" : [ "container1", "container2" ],
  "fabric" : [ "prod-xyz1", "prod-xyz2", "prod-xyz3" ]
};

export const dimensions = [ "All", "fabric", "container", "host" ];

export const granularities = [ "5_MINUTES", "HOURS", "DAYS" ];

export default {
  filters,
  dimensions,
  granularities
};
