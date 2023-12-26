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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.math.IntRange;
import org.apache.pinot.controller.recommender.data.DataGenerationHelpers;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sep 12, 2014
 */
// TODO: add DATE_TIME to the data generator
public class DataGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);

  DataGeneratorSpec _genSpec;

  private final Map<String, Generator> _generators;

  public DataGenerator() {
    _generators = new HashMap<String, Generator>();
  }

  public void init(DataGeneratorSpec spec)
      throws IOException {
    _genSpec = spec;

    for (final String column : _genSpec.getColumns()) {
      DataType dataType = _genSpec.getDataTypeMap().get(column);

      Generator generator;
      if (_genSpec.getPatternMap().containsKey(column)) {
        generator = GeneratorFactory
            .getGeneratorFor(PatternType.valueOf(_genSpec.getPatternMap().get(column).get("type").toString()),
                _genSpec.getPatternMap().get(column));
      } else if (_genSpec.getCardinalityMap().containsKey(column)) {
        generator = GeneratorFactory
            .getGeneratorFor(dataType, _genSpec.getCardinalityMap().get(column), _genSpec.getMvCountMap().get(column),
                _genSpec.getLengthMap().get(column), _genSpec.getTimeUnitMap().get(column));
      } else if (_genSpec.getRangeMap().containsKey(column)) {
        IntRange range = _genSpec.getRangeMap().get(column);
        generator = GeneratorFactory.getGeneratorFor(dataType, range.getMinimumInteger(), range.getMaximumInteger());
      } else {
        LOGGER.error("cardinality for this column does not exist : " + column);
        throw new RuntimeException("cardinality for this column does not exist");
      }
      generator.init();
      _generators.put(column, generator);
    }
  }

  /*
   * Returns a LinkedHashMap of columns and their respective generated values.
   * This ensures that the entries are ordered as per the column list
   *
   * */
  public Map<String, Object> nextRow() {
    Map<String, Object> row = new LinkedHashMap<>();
    for (String key : _genSpec.getColumns()) {
      row.put(key, _generators.get(key).next());
    }
    return row;
  }

  public Schema fetchSchema() {
    final Schema schema = new Schema();
    for (final String column : _genSpec.getColumns()) {
      final FieldSpec spec = buildSpec(_genSpec, column);
      schema.addField(spec);
    }
    return schema;
  }

  private FieldSpec buildSpec(DataGeneratorSpec genSpec, String column) {
    DataType dataType = genSpec.getDataTypeMap().get(column);
    FieldType fieldType = genSpec.getFieldTypeMap().get(column);

    FieldSpec spec;
    switch (fieldType) {
      case DIMENSION:
        spec = new DimensionFieldSpec();
        break;

      case METRIC:
        spec = new MetricFieldSpec();
        break;

      case TIME:
        spec = new TimeFieldSpec(new TimeGranularitySpec(dataType, genSpec.getTimeUnitMap().get(column), column));
        break;

      default:
        throw new RuntimeException("Invalid Field type.");
    }

    spec.setName(column);
    spec.setDataType(dataType);
    spec.setSingleValueField(true);

    return spec;
  }

  public static void main(String[] args)
      throws Exception {

    final Map<String, DataType> dataTypes = new HashMap<>();
    final Map<String, FieldType> fieldTypes = new HashMap<>();
    final Map<String, TimeUnit> timeUnits = new HashMap<>();

    final Map<String, Integer> cardinality = new HashMap<>();
    final Map<String, IntRange> range = new HashMap<>();
    final Map<String, Map<String, Object>> template = new HashMap<>();
    Map<String, Double> mvCountMap = new HashMap<>();
    Map<String, Integer> lengthMap = new HashMap<>();
    List<String> columnNames = new ArrayList<>();

    int cardinalityValue = 5;
    int strLength = 5;

    String colName = "colInt";
    dataTypes.put(colName, DataType.INT);
    fieldTypes.put(colName, FieldType.DIMENSION);
    cardinality.put(colName, cardinalityValue);
    columnNames.add(colName);
    mvCountMap.put(colName, 3.7);

    String colName2 = "colFloat";
    dataTypes.put(colName2, DataType.FLOAT);
    fieldTypes.put(colName2, FieldType.DIMENSION);
    cardinality.put(colName2, cardinalityValue);
    columnNames.add(colName2);
    mvCountMap.put(colName2, 3.7);

    String colName3 = "colString";
    dataTypes.put(colName3, DataType.STRING);
    fieldTypes.put(colName3, FieldType.DIMENSION);
    cardinality.put(colName3, cardinalityValue);
    columnNames.add(colName3);
    mvCountMap.put(colName3, 3.7);
    lengthMap.put(colName3, strLength);

    String colName4 = "metric";
    dataTypes.put(colName4, DataType.DOUBLE);
    fieldTypes.put(colName4, FieldType.METRIC);
    cardinality.put(colName4, cardinalityValue);
    columnNames.add(colName4);

    String colName5 = "colBytes";
    dataTypes.put(colName5, DataType.BYTES);
    fieldTypes.put(colName5, FieldType.DIMENSION);
    cardinality.put(colName5, cardinalityValue);
    columnNames.add(colName5);
    mvCountMap.put(colName5, 3.7);
    lengthMap.put(colName5, strLength + 1);

    for (int i = 0; i < 2; i++) {
      colName = "colString" + (i + 2);
      dataTypes.put(colName, DataType.STRING);
      fieldTypes.put(colName, FieldType.DIMENSION);
      cardinality.put(colName, cardinalityValue);
      columnNames.add(colName);
      lengthMap.put(colName, strLength + i + 2);
    }

    String outputDir = Paths.get(System.getProperty("java.io.tmpdir"), "csv-data").toString();
    final DataGeneratorSpec spec =
        new DataGeneratorSpec(columnNames, cardinality, range, template, mvCountMap, lengthMap, dataTypes, fieldTypes,
            timeUnits);

    final DataGenerator gen = new DataGenerator();
    gen.init(spec);
    DataGenerationHelpers.generateCsv(gen, 100, 1, outputDir, true);
    System.out.println("CSV data is generated under: " + outputDir);
  }
}
