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
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;


/**
 * Given a segments directory, pick a random segment and read the dictionaries for all dimension columns.
 */
public class SegmentDictionaryReader {
  private final String _segmentDirName;

  Map<String, Dictionary> _dictionaryMap;
  List<String> _dimensionColumns;
  List<String> _metricColumns;
  private Map<String, List<String>> _valuesMap;

  /**
   * Assumes that segments directory has at least one segment, and picks the first one.
   * - Gets all dimension/metric columns from the directory.
   * - Reads dictionaries for all dimension columns.
   *
   * @throws Exception
   * @param segmentDirName Name of directory containing tarred/untarred segments.
   */
  SegmentDictionaryReader(String segmentDirName)
      throws Exception {
    _segmentDirName = segmentDirName;

    File segmentsDir = new File(_segmentDirName);

    File segment = segmentsDir.listFiles()[0];

    if (segment.isFile()) {
      File tmpDir = File.createTempFile("query-comparison", null, new File("/tmp"));
      FileUtils.deleteQuietly(tmpDir);
      tmpDir.mkdir();
      TarGzCompressionUtils.unTar(segment, tmpDir);
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segment);
    Schema schema = segmentMetadata.getSchema();
    _dictionaryMap = new HashMap<>();

    _dimensionColumns = schema.getDimensionNames();
    _metricColumns = schema.getMetricNames();

    for (DimensionFieldSpec fieldSpec : schema.getDimensionFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        continue;
      }

      String column = fieldSpec.getName();
      IndexSegmentImpl indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(segment, ReadMode.heap);
      Dictionary dictionary = indexSegment.getDictionaryFor(column);
      _dictionaryMap.put(column, dictionary);
    }

    _valuesMap = new HashMap<>(_dimensionColumns.size());
    for (String column : _dimensionColumns) {
      Dictionary dictionary = _dictionaryMap.get(column);
      int numValues = dictionary.length();

      List<String> values = new ArrayList<>(numValues);
      for (int i = 0; i < numValues; ++i) {
        values.add(dictionary.get(i).toString());
      }
      _valuesMap.put(column, values);
    }
  }

  public List<String> getDimensionColumns() {
    return _dimensionColumns;
  }

  public List<String> getMetricColumns() {
    return _metricColumns;
  }

  /**
   * @return Map with key as column name, and value as list of (string) values for the column
   */
  public Map<String, List<String>> getValues() {
    return _valuesMap;
  }
}
