/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.index.metadata;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.data.Schema;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * The <code>SegmentMetadata</code> class holds the segment level management information and data statistics.
 */
@InterfaceAudience.Private
public interface SegmentMetadata {

  String getTableName();

  String getTimeColumn();

  long getStartTime();

  long getEndTime();

  TimeUnit getTimeUnit();

  Duration getTimeGranularity();

  Interval getTimeInterval();

  String getCrc();

  String getVersion();

  Schema getSchema();

  String getShardingKey();

  int getTotalDocs();

  File getIndexDir();

  String getName();

  long getIndexCreationTime();

  /**
   * Get the last time that this segment was pushed or <code>Long.MIN_VALUE</code> if it has never been pushed.
   */
  long getPushTime();

  /**
   * Get the last time that this segment was refreshed or <code>Long.MIN_VALUE</code> if it has never been refreshed.
   */
  long getRefreshTime();

  /**
   * Return the last time a record was indexed in this segment. Applicable for MutableSegments.
   *
   * @return time when the last record was indexed
   */
  long getLastIndexedTimestamp();

  /**
   * Return the latest ingestion timestamp associated with the records indexed in this segment.
   * Applicable for MutableSegments.
   *
   * @return latest timestamp associated with indexed records
   *         <code>Long.MIN_VALUE</code> if the stream doesn't provide a timestamp
   */
  long getLatestIngestionTimestamp();

  boolean hasDictionary(String columnName);

  String getForwardIndexFileName(String column);

  String getDictionaryFileName(String column);

  String getBitmapInvertedIndexFileName(String column);

  String getBitmapRangeIndexFileName(String column);

  String getBloomFilterFileName(String column);

  String getNullValueVectorFileName(String column);

  String getCreatorName();

  char getPaddingCharacter();

  boolean close();

  Map<String, String> getCustomMap();
}
