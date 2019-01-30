// rootcause dimensions table columns
export default [
  {
    propertyName: 'callee_container',
    title: 'Container',
    className: 'metrics-table__column metrics-table__column--text'
  }, {
    propertyName: 'callee_fabric',
    title: 'Fabric',
    className: 'metrics-table__column metrics-table__column--text'
  }, {
    propertyName: 'callee_api',
    title: 'API',
    className: 'metrics-table__column metrics-table__column--text'
  }, {
    propertyName: 'diffAverage',
    title: 'Change in Avg. Latency',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small',
    sortDirection: 'desc',
    sortPrecedence: 0
  }
];
