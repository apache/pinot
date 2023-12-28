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
  private final Map<String, String> _dateTimeFormatMap;
  private final Map<String, String> _dateTimeGranularityMap;

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

    _dateTimeFormatMap = new HashMap<>();
    _dateTimeGranularityMap = new HashMap<>();
  }

  public DataGeneratorSpec(List<String> columns, Map<String, Integer> cardinalityMap, Map<String, IntRange> rangeMap,
      Map<String, Map<String, Object>> patternMap, Map<String, Double> mvCountMap, Map<String, Integer> lengthMap,
      Map<String, DataType> dataTypesMap, Map<String, FieldType> fieldTypesMap, Map<String, TimeUnit> timeUnitMap,
      Map<String, String> dateTimeFormatMap, Map<String, String> dateTimeGranularityMap) {
    _columns = columns;
    _cardinalityMap = cardinalityMap;
    _rangeMap = rangeMap;
    _patternMap = patternMap;
    _mvCountMap = mvCountMap;
    _lengthMap = lengthMap;

    _dataTypeMap = dataTypesMap;
    _fieldTypeMap = fieldTypesMap;
    _timeUnitMap = timeUnitMap;
    _dateTimeGranularityMap = dateTimeGranularityMap;
    _dateTimeFormatMap = dateTimeFormatMap;
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

  public Map<String, String> getDateTimeFormatMap() {
    return _dateTimeFormatMap;
  }

  public Map<String, String> getDateTimeGranularityMap() {
    return _dateTimeGranularityMap;
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

  public static class Builder {
    private List<String> _columns = new ArrayList<>();
    private Map<String, Integer> _cardinalityMap = new HashMap<>();
    private Map<String, IntRange> _rangeMap = new HashMap<>();
    private Map<String, Map<String, Object>> _patternMap = new HashMap<>();
    private Map<String, Double> _mvCountMap = new HashMap<>();
    private Map<String, Integer> _lengthMap = new HashMap<>();
    private Map<String, DataType> _dataTypeMap = new HashMap<>();
    private Map<String, FieldType> _fieldTypeMap = new HashMap<>();
    private Map<String, TimeUnit> _timeUnitMap = new HashMap<>();
    private Map<String, String> _dateTimeFormatMap = new HashMap<>();
    private Map<String, String> _dateTimeGranularityMap = new HashMap<>();

    public DataGeneratorSpec build() {
      return new DataGeneratorSpec(_columns, _cardinalityMap, _rangeMap, _patternMap, _mvCountMap, _lengthMap,
          _dataTypeMap, _fieldTypeMap, _timeUnitMap, _dateTimeFormatMap, _dateTimeGranularityMap);
    }

    public Builder setColumns(List<String> columns) {
      _columns = columns;
      return this;
    }

    public Builder setCardinalityMap(Map<String, Integer> cardinalityMap) {
      _cardinalityMap = cardinalityMap;
      return this;
    }

    public Builder setRangeMap(Map<String, IntRange> rangeMap) {
      _rangeMap = rangeMap;
      return this;
    }

    public Builder setPatternMap(Map<String, Map<String, Object>> patternMap) {
      _patternMap = patternMap;
      return this;
    }

    public Builder setMvCountMap(Map<String, Double> mvCountMap) {
      _mvCountMap = mvCountMap;
      return this;
    }

    public Builder setLengthMap(Map<String, Integer> lengthMap) {
      _lengthMap = lengthMap;
      return this;
    }

    public Builder setDataTypeMap(Map<String, DataType> dataTypeMap) {
      _dataTypeMap = dataTypeMap;
      return this;
    }

    public Builder setFieldTypeMap(Map<String, FieldType> fieldTypeMap) {
      _fieldTypeMap = fieldTypeMap;
      return this;
    }

    public Builder setTimeUnitMap(Map<String, TimeUnit> timeUnitMap) {
      _timeUnitMap = timeUnitMap;
      return this;
    }

    public Builder setDateTimeFormatMap(Map<String, String> dateTimeFormatMap) {
      _dateTimeFormatMap = dateTimeFormatMap;
      return this;
    }

    public Builder setDateTimeGranularityMap(Map<String, String> dateTimeGranularityMap) {
      _dateTimeGranularityMap = dateTimeGranularityMap;
      return this;
    }
  }
}
