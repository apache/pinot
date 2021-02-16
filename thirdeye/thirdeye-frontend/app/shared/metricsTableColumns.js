// rootcause metrics table columns mock
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
    propertyName: 'wo1w',
    template: 'custom/metrics-table-offset',
    sortedBy: 'sortable_wo1w',
    title: 'WoW',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'wo2w',
    template: 'custom/metrics-table-offset',
    sortedBy: 'sortable_wo2w',
    title: 'Wo2W',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'score',
    title: 'Outlier',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small',
    sortDirection: 'desc',
    sortPrecedence: 1
  },
  {
    template: 'custom/rca-metric-links',
    propertyName: 'links',
    title: 'Links',
    disableFiltering: true,
    disableSorting: true,
    className: 'metrics-table__column metrics-table__column--small'
  }
];
