// rootcause dimensions table columns
export default [
  {
    propertyName: 'isSelected',
    isHidden: true,
    sortDirection: 'desc',
    sortPrecedence: 0
  },
  {
    template: 'custom/table-checkbox',
    className: 'metrics-table__column metrics-table__column--checkbox'
  },
  {
    propertyName: 'label',
    title: 'Dimension',
    className: 'metrics-table__column metrics-table__column--large'
  },
  {
    propertyName: 'current',
    sortedBy: 'sortable_current',
    title: 'current',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    propertyName: 'baseline',
    sortedBy: 'sortable_baseline',
    title: 'baseline',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    template: 'custom/dimensions-table-change',
    propertyName: 'change',
    sortedBy: 'sortable_change',
    title: 'Percentage Change',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  },
  {
    template: 'custom/dimensions-table-change',
    propertyName: 'changeContribution',
    sortedBy: 'sortable_changeContribution',
    title: 'Change in Contribution',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small',
    sortDirection: 'desc',
    sortPrecedence: 1
  },
  {
    template: 'custom/dimensions-table-change',
    propertyName: 'contributionToChange',
    sortedBy: 'sortable_contributionToChange',
    title: 'Contribution to Change',
    disableFiltering: true,
    className: 'metrics-table__column metrics-table__column--small'
  }
];
