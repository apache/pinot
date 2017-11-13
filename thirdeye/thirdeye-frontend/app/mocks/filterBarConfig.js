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
    header: "Holiday",
    eventType: "holiday",
    inputs: [
      {
        label: "country",
        labelMapping: "countryCode",
        type: "dropdown"
      }, {
        label: "region",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Deployment",
    eventType: "deployment",
    inputs: [
      {
        label: "fabric",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Other Anomalies",
    eventType: "other anomalies",
    inputs: [
      {
        label: "resolution",
        type: "dropdown"
      }
    ]
  }
];
