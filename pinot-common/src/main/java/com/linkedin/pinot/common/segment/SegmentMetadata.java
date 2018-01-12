/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.segment;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * The <code>SegmentMetadata</code> class holds the segment level management information and data statistics.
 */
public interface SegmentMetadata {

  String getTableName();

  String getIndexType();

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

  int getTotalRawDocs();

  String getIndexDir();

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

  boolean hasDictionary(String columnName);

  boolean hasStarTree();

  @Nullable
  StarTreeMetadata getStarTreeMetadata();

  String getForwardIndexFileName(String column);

  String getDictionaryFileName(String column);

  String getBitmapInvertedIndexFileName(String column);

  @Nullable
  String getCreatorName();

  char getPaddingCharacter();

  int getHllLog2m();

  /**
   * Get the derived column name for the given original column and derived metric type.
   *
   * @param column original column name.
   * @param derivedMetricType derived metric type.
   * @return derived column name if exists.
   *         null if not.
   */
  @Nullable
  String getDerivedColumn(String column, MetricFieldSpec.DerivedMetricType derivedMetricType);

  Map<String, String> toMap();

  boolean close();
}
