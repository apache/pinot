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
package org.apache.pinot.tools.data.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.math.IntRange;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.readers.FileFormat;


/**
 * Sep 12, 2014
 */

public class DataGeneratorSpec {
  private final List<String> columns;
  private final Map<String, Integer> cardinalityMap;
  private final Map<String, IntRange> rangeMap;
  private final Map<String, Map<String, Object>> patternMap;
  private final Map<String, Double> mvCountMap; // map of column name to average number of values per entry
  private final Map<String, Integer> lengthMap; // map of column name to average length of th entry (used for string generator)

  private final Map<String, DataType> dataTypesMap;
  private final Map<String, FieldType> fieldTypesMap;
  private final Map<String, TimeUnit> timeUnitMap;

  private final FileFormat outputFileFormat;
  private final String outputDir;
  private final boolean overrideOutDir;

  public DataGeneratorSpec() {
    this(new ArrayList<String>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
        new HashMap<>(), new HashMap<>(), new HashMap<>(),
        FileFormat.AVRO, "/tmp/dataGen", true);
  }

  public DataGeneratorSpec(List<String> columns, Map<String, Integer> cardinalityMap, Map<String, IntRange> rangeMap,
      Map<String, Map<String, Object>> patternMap, Map<String, Double> mvCountMap, Map<String, Integer> lengthMap, Map<String, DataType> dataTypesMap, Map<String, FieldType> fieldTypesMap, Map<String, TimeUnit> timeUnitMap,
      FileFormat format, String outputDir, boolean override) {
    this.columns = columns;
    this.cardinalityMap = cardinalityMap;
    this.rangeMap = rangeMap;
    this.patternMap = patternMap;
    this.mvCountMap = mvCountMap;
    this.lengthMap = lengthMap;

    outputFileFormat = format;
    this.outputDir = outputDir;
    overrideOutDir = override;

    this.dataTypesMap = dataTypesMap;
    this.fieldTypesMap = fieldTypesMap;
    this.timeUnitMap = timeUnitMap;
  }

  public Map<String, DataType> getDataTypesMap() {
    return dataTypesMap;
  }

  public Map<String, FieldType> getFieldTypesMap() {
    return fieldTypesMap;
  }

  public Map<String, TimeUnit> getTimeUnitMap() {
    return timeUnitMap;
  }

  public boolean isOverrideOutDir() {
    return overrideOutDir;
  }

  public List<String> getColumns() {
    return columns;
  }

  public Map<String, Integer> getCardinalityMap() {
    return cardinalityMap;
  }

  public Map<String, IntRange> getRangeMap() {
    return rangeMap;
  }

  public Map<String, Map<String, Object>> getPatternMap() {
    return patternMap;
  }

  public Map<String, Double> getMvCountMap() {
    return mvCountMap;
  }

  public Map<String, Integer> getLengthMap() {
    return lengthMap;
  }

  public FileFormat getOutputFileFormat() {
    return outputFileFormat;
  }

  public String getOutputDir() {
    return outputDir;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    for (final String column : columns) {
      if (cardinalityMap.get(column) != null) {
        builder.append(column + " : " + cardinalityMap.get(column) + " : " + dataTypesMap.get(column));
      } else if (rangeMap.get(column) != null) {
        builder.append(column + " : " + rangeMap.get(column) + " : " + dataTypesMap.get(column));
      } else {
        builder.append(column + " : " + patternMap.get(column));
      }
    }
    builder.append("output file format : " + outputFileFormat);
    builder.append("output file format : " + outputDir);
    return builder.toString();
  }
}
