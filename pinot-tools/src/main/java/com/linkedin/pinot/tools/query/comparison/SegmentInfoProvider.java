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
package com.linkedin.pinot.tools.query.comparison;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;


/**
 * Given a segments directory, pick all segments and read the dictionaries for all single-value dimension columns.
 * Here we will treat time column (if exists) as a single-value dimension column.
 */
public class SegmentInfoProvider {
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String SEGMENT_INFO_PROVIDER = "segmentInfoProvider";

  private final List<String> _singleValueDimensionColumns;
  private final List<String> _metricColumns;
  private final Map<String, List<Object>> _singleValueDimensionValuesMap;

  /**
   * Assume that segments directory has at least one segment.
   * - Gets all single-value dimension/metric columns from the directory.
   * - Reads dictionaries for all single-value dimension columns.
   *
   * @param segmentDirName Name of directory containing tarred/untarred segments.
   * @throws Exception
   */
  public SegmentInfoProvider(String segmentDirName)
      throws Exception {
    Set<String> uniqueMetrics = new HashSet<>();
    Set<String> uniqueSingleValueDimensions = new HashSet<>();
    Map<String, Set<Object>> uniqueSingleValueDimensionValues = new HashMap<>();

    File segmentsDir = new File(segmentDirName);
    for (File segment : segmentsDir.listFiles()) {
      readOneSegment(segment, uniqueMetrics, uniqueSingleValueDimensions, uniqueSingleValueDimensionValues);
    }

    _singleValueDimensionColumns = new ArrayList<>(uniqueSingleValueDimensions);
    _metricColumns = new ArrayList<>(uniqueMetrics);
    _singleValueDimensionValuesMap = new HashMap<>(uniqueSingleValueDimensionValues.size());

    for (Map.Entry<String, Set<Object>> entry : uniqueSingleValueDimensionValues.entrySet()) {
      _singleValueDimensionValuesMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
  }

  /**
   * Read the metadata of the given segmentFile and collect:
   * - Unique metric columns
   * - Unique single-value dimension columns
   * - Unique values for each single-value dimension columns
   *
   * @param segmentFile segment file.
   * @param uniqueMetrics unique metric columns buffer.
   * @param uniqueSingleValueDimensions unique single-value dimension columns buffer.
   * @param singleValueDimensionValuesMap single-value dimension columns to unique values map buffer.
   * @throws Exception
   */
  private void readOneSegment(File segmentFile, Set<String> uniqueMetrics, Set<String> uniqueSingleValueDimensions,
      Map<String, Set<Object>> singleValueDimensionValuesMap)
      throws Exception {
    // Get segment directory from segment file (decompress if necessary).
    File segmentDir;
    File tmpDir = null;
    if (segmentFile.isFile()) {
      tmpDir = File.createTempFile(SEGMENT_INFO_PROVIDER, null, new File(TMP_DIR));
      FileUtils.deleteQuietly(tmpDir);
      tmpDir.mkdir();
      TarGzCompressionUtils.unTar(segmentFile, tmpDir);
      segmentDir = tmpDir.listFiles()[0];
    } else {
      segmentDir = segmentFile;
    }

    IndexSegment indexSegment = Loaders.IndexSegment.load(segmentDir, ReadMode.heap);
    Schema schema = indexSegment.getSegmentMetadata().getSchema();

    // Add time column if exists.
    String timeColumn = schema.getTimeColumnName();
    if (timeColumn != null) {
      uniqueSingleValueDimensions.add(timeColumn);
      loadValuesForSingleValueDimension(indexSegment, singleValueDimensionValuesMap, timeColumn);
    }

    // Add all metric columns.
    uniqueMetrics.addAll(schema.getMetricNames());

    // Add all single-value dimension columns.
    for (DimensionFieldSpec fieldSpec : schema.getDimensionFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        continue;
      }
      String column = fieldSpec.getName();
      uniqueSingleValueDimensions.add(column);
      loadValuesForSingleValueDimension(indexSegment, singleValueDimensionValuesMap, column);
    }

    if (tmpDir != null) {
      FileUtils.deleteQuietly(tmpDir);
    }
  }

  /**
   * Helper method to load values for a single-value dimension.
   *
   * @param indexSegment index segment.
   * @param singleValueDimensionValuesMap single-value dimension columns to unique values map buffer.
   * @param column single-value dimension name.
   */
  private void loadValuesForSingleValueDimension(IndexSegment indexSegment,
      Map<String, Set<Object>> singleValueDimensionValuesMap, String column) {
    Dictionary dictionary = indexSegment.getDataSource(column).getDictionary();

    Set<Object> values = singleValueDimensionValuesMap.get(column);
    if (values == null) {
      values = new HashSet<>();
      singleValueDimensionValuesMap.put(column, values);
    }

    int length = dictionary.length();
    for (int i = 0; i < length; i++) {
      values.add(dictionary.get(i));
    }
  }

  /**
   * Return the list of single-value dimension columns.
   *
   * @return single-value dimension columns.
   */
  public List<String> getSingleValueDimensionColumns() {
    return _singleValueDimensionColumns;
  }

  /**
   * Return the list of metric columns
   *
   * @return metric columns.
   */
  public List<String> getMetricColumns() {
    return _metricColumns;
  }

  /**
   * Return the map from single-value dimension names to values list for the column.
   *
   * @return map from single-value dimension names to values list for the column.
   */
  public Map<String, List<Object>> getSingleValueDimensionValuesMap() {
    return _singleValueDimensionValuesMap;
  }
}
