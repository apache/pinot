// rootcause event table columns mock
export default [
  {
    template: 'custom/table-checkbox',
    useFilter: false,
    mayBeHidden: false,
    className: 'events-table__column--checkbox'
  },
  {
    template: 'custom/table-label',
    propertyName: 'label',
    title: 'Event Name',
    className: 'events-table__column'
  },
  {
    propertyName: 'humanStart',
    title: 'Start',
    sortedBy: 'start',
    className: 'events-table__column--compact',
    disableFiltering: true
  },
  {
    propertyName: 'humanEnd',
    title: 'End',
    sortedBy: 'end',
    className: 'events-table__column--compact',
    disableFiltering: true
  }
];
