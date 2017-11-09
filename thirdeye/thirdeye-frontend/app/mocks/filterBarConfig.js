/**
 * Config file to generate filter bar on rca-poc
 * Each objects contains the following properties:
 *  1. header {string} - displayed event type on the filter bar header
 *  2. eventType {string} - type of event to filter by
 *  3. inputs {array} - array of objects, each specifying the sub-filters of an eventType
 */
export default [
  {
    header: "Holiday",
    eventType: "holiday",
    inputs: [
      {
        label: "country",
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
