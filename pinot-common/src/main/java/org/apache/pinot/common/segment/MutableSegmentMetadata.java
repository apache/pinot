package org.apache.pinot.common.segment;

/**
 * SegmentMetadata associated with mutable segments. Adds a few additional fields like indexing/ingestion timestamps.
 */
public interface MutableSegmentMetadata extends SegmentMetadata{

  /**
   * Return the last time a record was indexed in this segment. Applicable for MutableSegments.
   *
   * @return time when the last record was indexed
   */
  long getLastIndexedTime();

  /**
   * Return the latest ingestion timestamp associated with the records indexed in this segment.
   * Applicable for MutableSegments.
   *
   * @return latest timestamp associated with indexed records
   *         <code>Long.MIN_VALUE</code> if the stream/message doesn't provide a timestamp
   */
  long getLatestIngestionTimestamp();

  /**
   * Return the number of errors encountered while indexing records.
   */
  int getIndexingErrors();

  /**
   * Set the last indexed time.
   * @param timestamp time of the last successful indexed record
   */
  void setLastIndexedTime(long timestamp);

  /**
   * Set the last ingestion timestamp.
   * @param timestamp latest timestamp associated with the ingested records
   */
  void setLatestIngestionTimestamp(long timestamp);

  /**
   * Increment the number of indexing errors by 1.
   */
  void incrementIndexingErrors();
}
