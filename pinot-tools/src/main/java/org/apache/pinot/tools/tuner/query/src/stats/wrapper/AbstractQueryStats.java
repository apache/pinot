package org.apache.pinot.tools.tuner.query.src.stats.wrapper;

/**
 * Wrapper for query stats fields, for ex, time, entriesScannedInFilter, etc.
 */
public abstract class AbstractQueryStats {
  public abstract String toString();
  protected String _query;
}
