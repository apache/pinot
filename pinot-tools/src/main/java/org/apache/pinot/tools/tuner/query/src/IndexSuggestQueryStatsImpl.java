package org.apache.pinot.tools.tuner.query.src;

import javax.annotation.Nonnull;


public class IndexSuggestQueryStatsImpl extends BasicQueryStats {

  private String _tableNameWithoutType;
  private String _numEntriesScannedInFilter;
  private String _numEntriesScannedPostFilter;
  private String _query;

  public static final class Builder {
    private String _tableNameWithoutType;
    private String _numEntriesScannedInFilter;
    private String _numEntriesScannedPostFilter;
    private String _query;

    public Builder() {
    }

    @Nonnull
    Builder _query(@Nonnull String val) {
      _query = val;
      return this;
    }

    @Nonnull
    Builder _tableNameWithoutType(@Nonnull String val) {
      _tableNameWithoutType = val;
      return this;
    }

    @Nonnull
    Builder _numEntriesScannedInFilter(String val) {
      _numEntriesScannedInFilter = val;
      return this;
    }

    @Nonnull
    Builder _numEntriesScannedPostFilter(String val) {
      _numEntriesScannedPostFilter = val;
      return this;
    }

    @Nonnull
    public IndexSuggestQueryStatsImpl build() {
      return new IndexSuggestQueryStatsImpl(this);
    }
  }

  @Override
  public String toString() {
    return "IndexSuggestQueryStatsImpl{" + "_tableNameWithoutType='" + _tableNameWithoutType + '\''
        + ", _numEntriesScannedInFilter=" + _numEntriesScannedInFilter + ", _numEntriesScannedPostFilter="
        + _numEntriesScannedPostFilter + ", _query='" + _query + '\'' + '}';
  }

  private IndexSuggestQueryStatsImpl(Builder builder) {
    _tableNameWithoutType = builder._tableNameWithoutType;
    _numEntriesScannedInFilter = builder._numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = builder._numEntriesScannedPostFilter;
    _query = builder._query;
  }

  public String getQuery() {
    return _query;
  }

  public String getTableNameWithoutType() {
    return _tableNameWithoutType;
  }

  public String getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  public String getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }
}
