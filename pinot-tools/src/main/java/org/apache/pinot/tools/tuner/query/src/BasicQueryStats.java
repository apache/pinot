package org.apache.pinot.tools.tuner.query.src;

/**
 * Wrapper for query stats fields, for ex, time, entriesScannedInFilter, etc.
 */
public abstract class BasicQueryStats {
  public abstract String toString();
}
