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

package com.linkedin.pinot.tools.query.comparison;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
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
 * Given a segments directory, pick a random segment and read the dictionaries for all dimension columns.
 */
public class SegmentInfoProvider {
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String SEGMENT_INFO_PROVIDER = "segmentInfoProvider";
  private final String _segmentDirName;

  List<String> _dimensionColumns;
  List<String> _metricColumns;
  private Map<String, List<String>> _columnValuesMap;

  /**
   * Assumes that segments directory has at least one segment, and picks the first one.
   * - Gets all dimension/metric columns from the directory.
   * - Reads dictionaries for all dimension columns.
   *
   * @throws Exception
   * @param segmentDirName Name of directory containing tarred/untarred segments.
   */
  public SegmentInfoProvider(String segmentDirName)
      throws Exception {

    _segmentDirName = segmentDirName;
    File segmentsDir = new File(_segmentDirName);

    Set<String> uniqueDimensions = new HashSet<>();
    Set<String> uniqueMetrics = new HashSet<>();
    Map<String, Set<String>> uniqueColumnValues = new HashMap<>();

    for (File segment : segmentsDir.listFiles()) {
      readOneSegment(segment, uniqueDimensions, uniqueMetrics, uniqueColumnValues);
    }

    _dimensionColumns = new ArrayList<>(uniqueDimensions);
    _metricColumns = new ArrayList<>(uniqueMetrics);
    _columnValuesMap = new HashMap<>(uniqueColumnValues.size());

    for (Map.Entry<String, Set<String>> entry : uniqueColumnValues.entrySet()) {
      _columnValuesMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
  }

  /**
   * Read the metadata of the given segmentFile and collect:
   * - Unique dimension columns
   * - Unique metric columns
   * - Unique values for each column
   *
   * @param segmentFile
   * @param uniqueDimensions
   * @param uniqueMetrics
   * @throws Exception
   */
  private void readOneSegment(File segmentFile, Set<String> uniqueDimensions, Set<String> uniqueMetrics,
      Map<String, Set<String>> columnValuesMap)
      throws Exception {
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

    IndexSegmentImpl indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(segmentDir, ReadMode.heap);
    SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
    Schema schema = segmentMetadata.getSchema();

    uniqueDimensions.addAll(schema.getDimensionNames());
    uniqueMetrics.addAll(schema.getMetricNames());

    for (DimensionFieldSpec fieldSpec : schema.getDimensionFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        continue;
      }

      String column = fieldSpec.getName();
      Dictionary dictionary = indexSegment.getDictionaryFor(column);

      Set<String> values = columnValuesMap.get(column);
      if (values == null) {
        values = new HashSet<>();
        columnValuesMap.put(column, values);
      }

      int length = dictionary.length();
      for (int i = 0; i < length; i++) {
        values.add(dictionary.getStringValue(i));
      }
    }

    if (tmpDir != null) {
      FileUtils.deleteQuietly(tmpDir);
    }
  }

  /**
   * Returns the list of dimension columns
   * @return
   */
  public List<String> getDimensionColumns() {
    return _dimensionColumns;
  }

  /**
   * Returns the list of metric columns
   * @return
   */
  public List<String> getMetricColumns() {
    return _metricColumns;
  }

  /**
   * @return Map with key as column name, and value as list of (string) values for the column
   */
  public Map<String, List<String>> getColumnValuesMap() {
    return _columnValuesMap;
  }
}
