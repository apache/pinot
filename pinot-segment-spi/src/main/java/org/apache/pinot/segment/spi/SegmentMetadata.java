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
package org.apache.pinot.segment.spi;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.data.Schema;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * The <code>SegmentMetadata</code> class holds the segment level management information and data statistics.
 */
@InterfaceAudience.Private
public interface SegmentMetadata {

  /**
   * Returns the raw table name (without the type suffix).
   */
  @Deprecated
  String getTableName();

  String getName();

  String getTimeColumn();

  long getStartTime();

  long getEndTime();

  TimeUnit getTimeUnit();

  Duration getTimeGranularity();

  Interval getTimeInterval();

  String getCrc();

  SegmentVersion getVersion();

  Schema getSchema();

  int getTotalDocs();

  File getIndexDir();

  @Nullable
  String getCreatorName();

  long getIndexCreationTime();

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

  List<StarTreeV2Metadata> getStarTreeV2MetadataList();

  Map<String, String> getCustomMap();

  String getStartOffset();

  String getEndOffset();

  default NavigableSet<String> getAllColumns() {
    return getSchema().getColumnNames();
  }

  TreeMap<String, ColumnMetadata> getColumnMetadataMap();

  default ColumnMetadata getColumnMetadataFor(String column) {
    return getColumnMetadataMap().get(column);
  }

  /**
   * Removes a column from the segment metadata.
   */
  void removeColumn(String column);

  /**
   * Converts segment metadata to json.
   * @param columnFilter list only the columns in the set. Lists all the columns if the parameter value is null.
   * @return json representation of segment metadata.
   */
  JsonNode toJson(@Nullable Set<String> columnFilter);

  default boolean isMutableSegment() {
    return false;
  }
}
