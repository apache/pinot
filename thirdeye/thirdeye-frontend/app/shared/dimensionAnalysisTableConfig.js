const COLUMN_CLASS = 'rootcause-dimensions-table__column';

/**
 * Dynamic grouped headers for RCA advanced dimension analysis table.
 * Colspan for 'dimensions' group dependent on user-selected analysis 'depth' level.
 */
export const groupedHeaders = (advDimensionCount, overallChange) => [
  [
    { title: 'Top Anomalous Dimensions', className: 'rootcause-dimensions-table__header', colspan: advDimensionCount },
    { title: '', className: 'rootcause-dimensions-table__header', colspan: 2 },
    { title: `Overall Change ${overallChange}`, className: 'rootcause-dimensions-table__header', colspan: 1 },
    { title: '', className: 'rootcause-dimensions-table__header', colspan: 2 }
  ]
];

/**
 * Static columns for RCA advanced dimension analysis table. Dimension-specific columns are
 * appended to this array in the component (rootcause-dimensions)
 */
export const baseColumns = [
  {
    propertyName: 'baselineNum',
    title: 'Baseline',
    className: `${COLUMN_CLASS} ${COLUMN_CLASS}--med-width baseline`,
    disableFiltering: true
  },
  {
    propertyName: 'currentNum',
    title: 'Current',
    className: `${COLUMN_CLASS} ${COLUMN_CLASS}--med-width current`,
    disableFiltering: true
  },
  {
    propertyName: 'percentageChangeNum',
    component: 'custom/dimensions-table/percent-change',
    title: '% Change',
    className: `${COLUMN_CLASS} ${COLUMN_CLASS}--med-width`,
    disableFiltering: true
  },
  {
    propertyName: 'nodeSizeNum',
    title: 'Node Size',
    component: 'custom/dimensions-table/node-size',
    className: `${COLUMN_CLASS} ${COLUMN_CLASS}--med-width`,
    disableFiltering: true
  },
  {
    propertyName: 'costNum',
    title: 'Cost',
    component: 'custom/dimensions-table/change-bars',
    className: `${COLUMN_CLASS} ${COLUMN_CLASS}--bar-cell`,
    disableFiltering: true,
    sortDirection: 'desc',
    sortPrecedence: 0
  }
];

export default {
  groupedHeaders,
  baseColumns
};
