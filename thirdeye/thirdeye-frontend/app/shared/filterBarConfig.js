/**
 * Config file to generate filter bar on rca-poc
 * Each objects contains the following properties:
 *  1. header {string} - displayed event type on the filter bar header
 *  2. eventType {string} - type of event to filter by
 *  3. color {string} - color of the dot next to the header on the filter bar
 *  4. inputs {array} - array of objects, each specifying the sub-filters of an eventType
 *    a. label {string} - displayed name for the input
 *    b. labelMapping {string} - key value of label in the payload's attribute object that maps to the label
 *    c. type {string} - input type (i.e. dropdown, checkbox, drag)
 *  5. framework {string} rca framework name for loading indicator
 */
export default [
  {
    header: "Holidays",
    eventType: "holiday",
    framework: "eventHoliday",
    color: "green",
    inputs: [
      {
        label: "Country",
        labelMapping: "countryCode",
        type: "dropdown"
      }
    ]
  },
  {
    header: "GCN",
    eventType: "gcn",
    framework: "eventIssue",
    color: "orange",
    inputs: [
      {
        label: "Fabric",
        labelMapping: "fabrics",
        type: "dropdown"
      },
      {
        label: "Status",
        labelMapping: "status",
        type: "dropdown"
      },
      {
        label: "Priority",
        labelMapping: "priority",
        type: "dropdown"
      }
    ]
  },
  {
    header: "LiX",
    eventType: "lix",
    framework: "eventExperiment",
    color: "purple",
    inputs: [
      {
        label: "metrics",
        labelMapping: "metrics",
        type: "dropdown"
      },
      {
        label: "services",
        labelMapping: "services",
        type: "dropdown"
      },
      {
        label: "tags",
        labelMapping: "tags",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Deployments",
    eventType: "informed",
    framework: "eventDeployment",
    color: "red",
    inputs: [
      {
        label: "services",
        labelMapping: "services",
        type: "dropdown"
      },
      {
        label: "actions",
        labelMapping: "actions",
        type: "dropdown"
      },
      {
        label: "fabrics",
        labelMapping: "fabrics",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Anomalies",
    eventType: "anomaly",
    framework: "eventAnomaly",
    color: "teal",
    inputs: [
      {
        label: "dataset",
        labelMapping: "dataset",
        type: "dropdown"
      },
      {
        label: "metric",
        labelMapping: "metric",
        type: "dropdown"
      }
    ]
  }
];
