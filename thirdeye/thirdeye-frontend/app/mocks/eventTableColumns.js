// rootcause event table columns mock
export default [
  {
    template: 'custom/checkbox',
    useFilter: false,
    mayBeHidden: false,
    className: 'events-table__column--checkbox'
  },
  {
    propertyName: 'label',
    title: 'Event Name',
    className: 'events-table__column'
  },
  {
    propertyName: 'eventType',
    title: 'Type',
    filterWithSelect: true,
    sortFilterOptions: true,
    className: 'events-table__column--compact'
  },
  {
    propertyName: 'start',
    title: 'Start',
    className: 'events-table__column--compact',
    disableFiltering: true
  },
  {
    propertyName: 'end',
    title: 'End',
    className: 'events-table__column--compact',
    disableFiltering: true
  }
];
