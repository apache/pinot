// Columns for anomalies table
export default [
  {
    propertyName: 'start',
    template: 'custom/anomalies-table/start-duration',
    title: 'Start/Duration',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'dimensions',
    template: 'custom/anomalies-table/dimensions',
    title: 'Dimensions',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'severity',
    title: 'Severity Score',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'baseline',
    template: 'custom/anomalies-table/current-wow',
    title: 'Current/WoW',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'feedback',
    component: 'custom/anomalies-table/resolution',
    title: 'Resolution',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'link',
    template: 'custom/anomalies-table/investigation-link',
    title: 'Investigation Link',
    className: 'anomalies-table__column',
    disableFiltering: true
  }
];
