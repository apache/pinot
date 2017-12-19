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
