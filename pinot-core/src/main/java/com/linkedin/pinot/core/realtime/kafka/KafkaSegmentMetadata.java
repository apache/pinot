package com.linkedin.pinot.core.realtime.kafka;

import java.util.Map;

import org.apache.helix.ZNRecord;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
/**
 * Wrapper to encapsulate the metadata informatio for a real time segment based on Kafka
 * @author kgopalak
 *
 */
public class KafkaSegmentMetadata implements SegmentMetadata {

  public KafkaSegmentMetadata(ZNRecord record) {

  }

  @Override
  public String getResourceName() {
    return null;
  }

  @Override
  public String getTableName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getIndexType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Duration getTimeGranularity() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Interval getTimeInterval() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCrc() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Schema getSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getShardingKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getTotalDocs() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getIndexDir() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getIndexCreationTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getPushTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getRefreshTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<String, String> toMap() {
    // TODO Auto-generated method stub
    return null;
  }

@Override
public boolean hasDictionary(String columnName) {
	// TODO Auto-generated method stub
	return false;
}

public long getStartOffset() {
  // TODO Auto-generated method stub
  return 0;
}

public long getEndOffset() {
  // TODO Auto-generated method stub
  return 0;
}

}
