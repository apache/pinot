package com.linkedin.pinot.common.segment;

import java.util.Map;

import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;


/**
 * SegmentMetadata holds segment level management information and data
 * statistics.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface SegmentMetadata {
  /**
   * @return
   */
  public String getResourceName();

  /**
   * @return
   */
  public String getTableName();

  /**
   * @return
   */
  public String getIndexType();

  /**
   * @return
   */
  public Duration getTimeGranularity();

  /**
   * @return
   */
  public Interval getTimeInterval();

  /**
   * @return
   */
  public String getCrc();

  /**
   * @return
   */
  public String getVersion();

  /**
   * @return
   */
  public Schema getSchema();

  /**
   * @return
   */
  public String getShardingKey();

  /**
   * @return
   */
  public int getTotalDocs();

  /**
   * @return
   */
  public String getIndexDir();

  /**
   * @return
   */
  public String getName();

  /**
   *
   * @return
   */
  public long getIndexCreationTime();

  /**
   * Returns the last time that this segment was pushed or Long.MIN_VALUE if it has never been pushed.
   */
  public long getPushTime();

  /**
   * Returns the last time that this segment was refreshed or Long.MIN_VALUE if it has never been refreshed.
   */
  public long getRefreshTime();

  /**
   *
   * @return
   */
  public Map<String, String> toMap();
}
