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
package com.linkedin.pinot.tools.data.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.IntRange;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.core.data.readers.FileFormat;


/**
 * Sep 12, 2014
 */

public class DataGeneratorSpec {
  private final List<String> columns;
  private final Map<String, Integer> cardinalityMap;
  private final Map<String, IntRange> rangeMap;

  private final Map<String, DataType> dataTypesMap;
  private final Map<String, FieldType> fieldTypesMap;
  private final Map<String, TimeUnit> timeUnitMap;

  private final FileFormat outputFileFormat;
  private final String outputDir;
  private final boolean overrideOutDir;

  public DataGeneratorSpec() {
    this(new ArrayList<String>(), new HashMap<String, Integer>(), new HashMap<String, IntRange>(),
        new HashMap<String, DataType>(), new HashMap<String, FieldType>(), new HashMap<String, TimeUnit>(),
        FileFormat.AVRO, "/tmp/dataGen", true);
  }

  public DataGeneratorSpec(List<String> columns, Map<String, Integer> cardinalityMap,
      Map<String, IntRange> rangeMap, Map<String, DataType> dataTypesMap,
      Map<String, FieldType> fieldTypesMap, Map<String, TimeUnit> timeUnitMap,
      FileFormat format, String outputDir, boolean override) {
    this.columns = columns;
    this.cardinalityMap = cardinalityMap;
    this.rangeMap = rangeMap;
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
      } else {
        builder.append(column + " : " + rangeMap.get(column) + " : " + dataTypesMap.get(column));
      }
    }
    builder.append("output file format : " + outputFileFormat);
    builder.append("output file format : " + outputDir);
    return builder.toString();
  }
}
