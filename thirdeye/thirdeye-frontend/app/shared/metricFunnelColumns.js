// rootcause metrics funnel columns mock
export default [
  {
    propertyName: 'isSelected',
    isHidden: true,
    sortDirection: 'desc',
    sortPrecedence: 0
  },
  {
    template: 'custom/table-checkbox',
    className: 'metrics-table__column  metrics-table__column--checkbox'
  },
  {
    propertyName: 'label',
    title: 'Metric',
    template: 'custom/metrics-table-metric',
    className: 'metrics-table__column metrics-table__column--large'
  },
  {
    propertyName: 'current',
    template: 'custom/metrics-table-current',
    sortedBy: 'sortable_current',
    title: 'current',
    disableFiltering: true,
    disableSorting: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'baseline',
    template: 'custom/metrics-table-offset',
    sortedBy: 'sortable_baseline',
    title: 'baseline',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'yo1y',
    template: 'custom/metrics-table-offset',
    sortedBy: 'sortable_yo1y',
    title: 'YoY',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'interval',
    title: 'Interval',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'inInterval',
    title: 'In Interval',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  }
];
