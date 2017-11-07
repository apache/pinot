/**
 * Config file to generate filter bar on rca-poc
 */
export default [
  {
    eventType: "Holiday",
    inputs: [
      {
        label: "country",
        type: "dropdown"
      }
    ]
  },
  {
    eventType: "Deployment",
    inputs: [
      {
        label: "services",
        type: "dropdown"
      }
    ]
  },
  {
    eventType: "Other Anomalies",
    inputs: [
      {
        label: "resolution",
        type: "dropdown"
      }
    ]
  }
];
