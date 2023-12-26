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
package org.apache.pinot.controller.recommender.data.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.math.IntRange;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.readers.FileFormat;


public class DataGeneratorSpec {
  private final List<String> _columns;
  private final Map<String, Integer> _cardinalityMap;
  private final Map<String, IntRange> _rangeMap;
  private final Map<String, Map<String, Object>> _patternMap;
  private final Map<String, Double> _mvCountMap; // map of column name to average number of values per entry
  private final Map<String, Integer> _lengthMap;
  // map of column name to average length of th entry (used for string/byte generator)

  private final Map<String, DataType> _dataTypeMap;
  private final Map<String, FieldType> _fieldTypeMap;
  private final Map<String, TimeUnit> _timeUnitMap;

  @Deprecated
  private FileFormat _outputFileFormat;
  @Deprecated
  private String _outputDir;
  @Deprecated
  private boolean _overrideOutDir;

  @Deprecated
  public DataGeneratorSpec() {
    this(new ArrayList<String>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
        new HashMap<>(), new HashMap<>(), new HashMap<>(), FileFormat.AVRO, "/tmp/dataGen", true);
  }

  @Deprecated
  public DataGeneratorSpec(List<String> columns, Map<String, Integer> cardinalityMap, Map<String, IntRange> rangeMap,
      Map<String, Map<String, Object>> patternMap, Map<String, Double> mvCountMap, Map<String, Integer> lengthMap,
      Map<String, DataType> dataTypesMap, Map<String, FieldType> fieldTypesMap, Map<String, TimeUnit> timeUnitMap,
      FileFormat format, String outputDir, boolean override) {
    _columns = columns;
    _cardinalityMap = cardinalityMap;
    _rangeMap = rangeMap;
    _patternMap = patternMap;
    _mvCountMap = mvCountMap;
    _lengthMap = lengthMap;

    _outputFileFormat = format;
    _outputDir = outputDir;
    _overrideOutDir = override;

    _dataTypeMap = dataTypesMap;
    _fieldTypeMap = fieldTypesMap;
    _timeUnitMap = timeUnitMap;
  }

  public DataGeneratorSpec(List<String> columns, Map<String, Integer> cardinalityMap, Map<String, IntRange> rangeMap,
      Map<String, Map<String, Object>> patternMap, Map<String, Double> mvCountMap, Map<String, Integer> lengthMap,
      Map<String, DataType> dataTypesMap, Map<String, FieldType> fieldTypesMap, Map<String, TimeUnit> timeUnitMap) {
    _columns = columns;
    _cardinalityMap = cardinalityMap;
    _rangeMap = rangeMap;
    _patternMap = patternMap;
    _mvCountMap = mvCountMap;
    _lengthMap = lengthMap;

    _dataTypeMap = dataTypesMap;
    _fieldTypeMap = fieldTypesMap;
    _timeUnitMap = timeUnitMap;
  }

  public Map<String, DataType> getDataTypeMap() {
    return _dataTypeMap;
  }

  public Map<String, FieldType> getFieldTypeMap() {
    return _fieldTypeMap;
  }

  public Map<String, TimeUnit> getTimeUnitMap() {
    return _timeUnitMap;
  }

  public boolean isOverrideOutDir() {
    return _overrideOutDir;
  }

  public List<String> getColumns() {
    return _columns;
  }

  public Map<String, Integer> getCardinalityMap() {
    return _cardinalityMap;
  }

  public Map<String, IntRange> getRangeMap() {
    return _rangeMap;
  }

  public Map<String, Map<String, Object>> getPatternMap() {
    return _patternMap;
  }

  public Map<String, Double> getMvCountMap() {
    return _mvCountMap;
  }

  public Map<String, Integer> getLengthMap() {
    return _lengthMap;
  }

  public FileFormat getOutputFileFormat() {
    return _outputFileFormat;
  }

  public String getOutputDir() {
    return _outputDir;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    for (final String column : _columns) {
      if (_cardinalityMap.get(column) != null) {
        builder.append(column + " : " + _cardinalityMap.get(column) + " : " + _dataTypeMap.get(column));
      } else if (_rangeMap.get(column) != null) {
        builder.append(column + " : " + _rangeMap.get(column) + " : " + _dataTypeMap.get(column));
      } else {
        builder.append(column + " : " + _patternMap.get(column));
      }
      builder.append(", ");
    }
    builder.append("output file format : " + _outputFileFormat);
    builder.append(", output dir : " + _outputDir);
    return builder.toString();
  }
}
