/**
 * Columns for RCA advanced dimension analysis table
 * NOTE: hard-coded in first iteration
 */
export default [
  {
    propertyName: 'country',
    title: 'Country',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'platform',
    title: 'Platform',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'cob',
    title: 'Current/Baseline',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'overallChange',
    title: 'Contribution to Overall Change',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'percentChange',
    title: '% Change',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'contributionChange',
    title: 'Change in Contribution',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  }
];
