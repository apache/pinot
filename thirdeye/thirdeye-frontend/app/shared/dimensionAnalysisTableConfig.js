/**
 * Dynamic grouped headers for RCA advanced dimension analysis table.
 * Colspan for 'dimensions' group dependent on user-selected analysis 'depth' level.
 */
export const groupedHeaders = (advDimensionCount, overallChange) => [
  [
    {title: 'Dimensions', class: 'advanced-dimensions-table__header', colspan: advDimensionCount},
    {title: '', class: 'advanced-dimensions-table__header', colspan: 1},
    {title: `Overall Change ${overallChange}`, class: 'advanced-dimensions-table__header', colspan: 1},
    {title: '', class: 'advanced-dimensions-table__header', colspan: 2}
  ]
];

/**
 * Static columns for RCA advanced dimension analysis table. Dimension-specific columns are
 * appended to this array in the component (rootcause-dimensions)
 */
export const baseColumns = [
  {
    propertyName: 'cob',
    title: 'Current/Baseline',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'contributionToOverallChange',
    title: 'Contribution to Overall Change',
    className: 'advanced-dimensions-table__column advanced-dimensions__column--med-width',
    disableFiltering: true
  },
  {
    propertyName: 'percentageChange',
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

export default {
  groupedHeaders,
  baseColumns
}
