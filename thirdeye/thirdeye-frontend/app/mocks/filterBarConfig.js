/**
 * Config file to generate filter bar on rca-poc
 */
export default [
  {
    header: "Holiday",
    inputs: [
      {
        label: "country",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Deployment",
    inputs: [
      {
        label: "services",
        type: "dropdown"
      }
    ]
  },
  {
    header: "Other Anomalies",
    inputs: [
      {
        label: "resolution",
        type: "dropdown"
      }
    ]
  }
];
