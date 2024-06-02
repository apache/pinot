package org.apache.pinot.spi.data;

public class SchemaInfo {
  String schemaName;
  int dimensionFieldSpecsCount;
  int dateTimeFieldSpecsCount;
  int metricFieldSpecsCount;

  /**
   * This class gives the details of a particular schema and the column metrics
   * @param schemaName name of the schema
   * @param dimensionFieldSpecsCount corresponds to dimensions column count
   * @param dateTimeFieldSpecsCount corresponds to date time fields column count
   * @param metricFieldSpecsCount corresponds to metric fields column count
   */
  public SchemaInfo(String schemaName, int dimensionFieldSpecsCount, int dateTimeFieldSpecsCount,
      int metricFieldSpecsCount) {
    this.schemaName = schemaName;
    this.dimensionFieldSpecsCount = dimensionFieldSpecsCount;
    this.dateTimeFieldSpecsCount = dateTimeFieldSpecsCount;
    this.metricFieldSpecsCount = metricFieldSpecsCount;
  }
}
