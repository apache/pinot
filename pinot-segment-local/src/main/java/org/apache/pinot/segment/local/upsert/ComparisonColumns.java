package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nonnull;


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonColumns implements Comparable {
  private ComparisonColumns _other = null;
  private final Map<String, ComparisonColumn> _comparisonColumns;

  public ComparisonColumns(Map<String, ComparisonColumn> comparisonColumns) {
    _comparisonColumns = comparisonColumns;
  }

  public Map<String, ComparisonColumn> getComparisonColumns() {
    return _comparisonColumns;
  }

  public ComparisonColumns getOther() {
    return _other;
  }

  @Override
  public int compareTo(@Nonnull Object other) {

    Preconditions.checkState(other instanceof ComparisonColumns,
        "ComparisonColumns is only Comparable with another instance of ComparisonColumns");

    /* Capture other for later use in {@link RecordInfo#getComparisonValue()} */
    _other = (ComparisonColumns) other;
    for (Map.Entry<String, ComparisonColumn> columnEntry : _comparisonColumns.entrySet()) {
      ComparisonColumn comparisonColumn = columnEntry.getValue();
      // "this" may only 1 non-null value at most. _other may have all non-null values, however.
      if (comparisonColumn.isNull) {
        continue;
      }

      ComparisonColumn otherComparisonColumn = _other.getComparisonColumns().get(comparisonColumn.columnName);
      if (otherComparisonColumn == null) {
        // This can happen if a new column is added to the list of coparisonColumns. We want o support that without
        // requiring a server restart, so handle the null here.
        return 1;
      }
      return comparisonColumn.comparisonValue.compareTo(otherComparisonColumn.comparisonValue);
    }

    return -1;
  }
}
