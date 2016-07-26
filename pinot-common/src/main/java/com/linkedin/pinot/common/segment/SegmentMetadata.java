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

import java.io.File;
import java.util.Map;

import javax.annotation.Nullable;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;


/**
 * SegmentMetadata holds segment level management information and data
 * statistics.
 */
public interface SegmentMetadata {
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
   *
   * @return
   */
  int getTotalRawDocs();

  /**
   * @return
   */
  public String getIndexDir();

  /**
   * @return
   */
  public String getName();

  /**
   * @return
   */
  public long getIndexCreationTime();

  /**
   * Returns the last time that this segment was pushed or Long.MIN_VALUE if it has never been
   * pushed.
   */
  public long getPushTime();

  /**
   * Returns the last time that this segment was refreshed or Long.MIN_VALUE if it has never been
   * refreshed.
   */
  public long getRefreshTime();

  /**
   * Returns if a column has dictionary or not.
   */
  public boolean hasDictionary(String columnName);

  /** Returns true if the segment has a StarTree index defined */
  public boolean hasStarTree();

  /**
   * Returns the StarTreeMetadata for the segment
   * @return
   */
  public StarTreeMetadata getStarTreeMetadata();

  /**
   * returns the forward Index file name with appropriate extension for a given version
   * @param column
   * @return
   */
  String getForwardIndexFileName(String column, String segmentVersion);

  /**
   * returns the dictionary file name with appropriate extension for a given version
   * @param column
   * @return
   */
  String getDictionaryFileName(String column, String segmentVersion);

  /**
   * returns the bitmap inverted index file name with appropriate extension for a given version
   * @param column
   * @return
   */
  String getBitmapInvertedIndexFileName(String column, String segmentVersion);

  /**
   * returns the name of the component that created the segment
   * @return
   */
  @Nullable
  String getCreatorName();

  /**
   * returns the padding character
   * @return
   */
  Character getPaddingCharacter();

  /**
   * @return
   */
  public Map<String, String> toMap();

  public boolean close();

}
