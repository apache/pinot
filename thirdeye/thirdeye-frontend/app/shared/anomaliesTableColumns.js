// Columns for anomalies table
export default [
  {
    propertyName: 'start',
    title: 'Start/Duration',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'dimensions',
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
    title: 'Current/WoW',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    propertyName: 'resolution',
    title: 'Resolution',
    className: 'anomalies-table__column',
    disableFiltering: true
  },
  {
    title: 'Investigation Link',
    className: 'anomalies-table__column',
    disableFiltering: true
  }
];
