package org.apache.pinot.segment.local.upsert;

@SuppressWarnings({"rawtypes"})
public class ComparisonColumn {
  public final String columnName;
  public final Comparable comparisonValue;
  public final boolean isNull;

  public ComparisonColumn(String columnName, Comparable comparisonValue) {
    this(columnName, comparisonValue, false);
  }
  public ComparisonColumn(String columnName, Comparable comparisonValue, boolean isNull) {
    this.columnName = columnName;
    this.comparisonValue = comparisonValue;
    this.isNull = isNull;
  }
}

