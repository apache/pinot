package com.linkedin.pinot.index.segment;

import org.joda.time.Interval;

import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.time.TimeGranularity;


/**
 * SegmentMetadata holds segment level management information and data statistics.
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
   * @param resourceName
   */
  public void setResourceName(String resourceName);

  /**
   * @return
   */
  public String getTableName();

  /**
   * @param tableName
   */
  public void setTableName(String tableName);

  /**
   * @return
   */
  public String getIndexType();

  /**
   * @return
   */
  public TimeGranularity getTimeGranularity();

  /**
   * @param timeGranularity
   */
  public void setTimeGranularity(TimeGranularity timeGranularity);

  /**
   * @return
   */
  public Interval getTimeInterval();

  /**
   * @param timeInterval
   */
  public void setTimeInterval(Interval timeInterval);

  /**
   * @return
   */
  public String getCrc();

  /**
   * @param crc
   */
  public void setCrc(String crc);

  /**
   * @return
   */
  public String getVersion();

  /**
   * @param version
   */
  public void setVersion(String version);

  /**
   * @return
   */
  public Schema getSchema();

  /**
   * @param schema
   */
  public void setSchema(Schema schema);

  /**
   * @return
   */
  public String getShardingKey();

  /**
   * @param shardingKey
   */
  public void setShardingKey(String shardingKey);
}
