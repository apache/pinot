package org.apache.pinot.tools.tuner.query.src.stats.wrapper;

import javax.annotation.Nonnull;


/**
 * The query stats used for inverted index study and recommendation
 */
public class IndexSuggestQueryStatsImpl extends AbstractQueryStats {

  private String _time;
  private String _numEntriesScannedInFilter;
  private String _numEntriesScannedPostFilter;

  public static final class Builder {
    private String _tableNameWithoutType = null;
    private String _numEntriesScannedInFilter = null;
    private String _numEntriesScannedPostFilter = null;
    private String _query = null;
    private String _time = null;

    public Builder() {
    }

    /**
     *
     * @param val Query text
     * @return
     */
    @Nonnull
    public Builder setQuery(@Nonnull String val) {
      _query = val;
      return this;
    }

    @Nonnull
    public Builder setTableNameWithoutType(@Nonnull String val) {
      _tableNameWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder setNumEntriesScannedInFilter(String val) {
      _numEntriesScannedInFilter = val;
      return this;
    }

    @Nonnull
    public Builder setNumEntriesScannedPostFilter(String val) {
      _numEntriesScannedPostFilter = val;
      return this;
    }

    @Nonnull
    public IndexSuggestQueryStatsImpl build() {
      return new IndexSuggestQueryStatsImpl(this);
    }

    @Nonnull
    public Builder setTime(@Nonnull String val) {
      _time = val;
      return this;
    }
  }

  @Override
  public String toString() {
    return "IndexSuggestQueryStatsImpl{" + "_time='" + _time + '\'' + ", _tableNameWithoutType='"
        + _tableNameWithoutType + '\'' + ", _numEntriesScannedInFilter='" + _numEntriesScannedInFilter + '\''
        + ", _numEntriesScannedPostFilter='" + _numEntriesScannedPostFilter + '\'' + ", _query='" + _query + '\'' + '}';
  }

  private IndexSuggestQueryStatsImpl(Builder builder) {
    _time = builder._time;
    _tableNameWithoutType = builder._tableNameWithoutType;
    _numEntriesScannedInFilter = builder._numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = builder._numEntriesScannedPostFilter;
    _query = builder._query;
  }

  public String getQuery() {
    return _query;
  }

  public String getTime() {
    return _time;
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
