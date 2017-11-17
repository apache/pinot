/**
 * Config file to generate filter bar on rca-poc
 * Each objects contains the following properties:
 *  1. header {string} - displayed event type on the filter bar header
 *  2. eventType {string} - type of event to filter by
 *  3. inputs {array} - array of objects, each specifying the sub-filters of an eventType
 *    a. label {string} - displayed name for the input
 *    b. labelMapping {string} - key value of label in the payload's attribute object that maps to the label
 *    c. type {string} - input type (i.e. dropdown, checkbox, drag)
 */
export default [
  {
    header: "Anomalies",
    eventType: "anomaly",
    inputs: [
      {
        label: "dataset",
        type: "dropdown"
      },
      {
        label: "metric",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Holidays",
    eventType: "holiday",
    inputs: [
      {
        label: "Country",
        labelMapping: "countryCode",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Issues",
    eventType: "gcn",
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
      },
    ]
  },
  {
    header: "Experiments",
    eventType: "lix",
    inputs: [
      {
        label: "metrics",
        type: "dropdown"
      },
      {
        label: "services",
        type: "dropdown"
      },
      {
        label: "tags",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Deployments",
    eventType: "informed",
    inputs: [
      {
        label: "services",
        type: "dropdown"
      },
      {
        label: "actions",
        type: "dropdown"
      },
      {
        label: "fabrics",
        type: "dropdown"
      }
    ]
  }
];
