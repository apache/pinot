/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.kafka;

import java.util.Map;

import org.apache.helix.ZNRecord;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;


/**
 * Wrapper to encapsulate the metadata informatio for a real time segment based on Kafka
 *
 */
public class KafkaSegmentMetadata implements SegmentMetadata {

  public KafkaSegmentMetadata(ZNRecord record) {

  }

  @Override
  public String getTableName() {
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
  public int getTotalAggregateDocs() {
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

  @Override
  public boolean close() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean hasStarTree() {
    return false;
  }
}
