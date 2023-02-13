package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nonnull;


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonColumns implements Comparable {
  private ComparisonColumns _other = null;
  private final Map<String, ComparisonValue> _comparisonColumns;

  public ComparisonColumns(Map<String, ComparisonValue> comparisonColumns) {
    _comparisonColumns = comparisonColumns;
  }

  public Map<String, ComparisonValue> getComparisonColumns() {
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
    int numNull = 0;
    for (Map.Entry<String, ComparisonValue> columnEntry : _comparisonColumns.entrySet()) {
      ComparisonValue comparisonValue = columnEntry.getValue();
      // "this" may have at most 1 non-null value. _other may have all non-null values, however.
      if (comparisonValue.isNull()) {
        numNull++;
        continue;
      }

      ComparisonValue otherComparisonValue = _other.getComparisonColumns().get(columnEntry.getKey());
      if (otherComparisonValue == null) {
        // This can happen if a new column is added to the list of coparisonColumns. We want o support that without
        // requiring a server restart, so handle the null here.
        return 1;
      }
      return comparisonValue.getComparisonValue().compareTo(otherComparisonValue.getComparisonValue());
    }

    return -1;
  }
}
