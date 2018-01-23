// rootcause event table columns mock
export default [
  {
    template: 'custom/table-checkbox',
    useFilter: false,
    mayBeHidden: false,
    className: 'events-table__column events-table__column--checkbox'
  },
  {
    template: 'custom/table-label',
    propertyName: 'label',
    title: 'Event Name',
    className: 'events-table__column'
  },
  {
    propertyName: 'humanStart',
    title: 'Start Time',
    sortedBy: 'start',
    className: 'events-table__column events-table__column--compact',
    disableFiltering: true
  },
  {
    propertyName: 'duration',
    title: 'Duration',
    sortedBy: 'duration',
    className: 'events-table__column events-table__column--compact',
    disableFiltering: true
  }
];
