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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.readers.FileFormat;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.IntRange;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sep 12, 2014
 */

public class DataGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);
  private File outDir;

  DataGeneratorSpec genSpec;

  private final Map<String, Generator> generators;

  public DataGenerator() {
    generators = new HashMap<String, Generator>();
  }

  public void init(DataGeneratorSpec spec) throws IOException {
    genSpec = spec;
    outDir = new File(genSpec.getOutputDir());
    if (outDir.exists() && !genSpec.isOverrideOutDir()) {
      LOGGER.error("output directory already exists, and override is set to false");
      throw new RuntimeException("output directory exists");
    }

    if (outDir.exists()) {
      FileUtils.deleteDirectory(outDir);
    }

    outDir.mkdir();

    for (final String column : genSpec.getColumns()) {
      DataType dataType = genSpec.getDataTypesMap().get(column);

      if (genSpec.getCardinalityMap().containsKey(column)) {
        generators.put(column,
            GeneratorFactory.getGeneratorFor(dataType, genSpec.getCardinalityMap().get(column)));

      } else if (genSpec.getRangeMap().containsKey(column)) {
        IntRange range = genSpec.getRangeMap().get(column);

        generators.put(column,
            GeneratorFactory.getGeneratorFor(dataType, range.getMinimumInteger(), range.getMaximumInteger()));
      } else {
        LOGGER.error("cardinality for this column does not exist : " + column);
        throw new RuntimeException("cardinality for this column does not exist");
      }

      generators.get(column).init();
    }
  }

  public void generate(long totalDocs, int numFiles) throws IOException, JSONException {
    final int numPerFiles = (int) (totalDocs / numFiles);
    for (int i = 0; i < numFiles; i++) {
      try (AvroWriter writer = new AvroWriter(outDir, i, generators, fetchSchema())) {
        for (int j = 0; j < numPerFiles; j++) {
          writer.writeNext();
        }
      }
    }
  }

  public Schema fetchSchema() {
    final Schema schema = new Schema();
    for (final String column : genSpec.getColumns()) {
      final FieldSpec spec = buildSpec(genSpec, column);
      schema.addField(spec);
    }
    return schema;
  }

  private FieldSpec buildSpec(DataGeneratorSpec genSpec, String column) {
    DataType dataType = genSpec.getDataTypesMap().get(column);
    FieldType fieldType = genSpec.getFieldTypesMap().get(column);

    FieldSpec spec;
    switch (fieldType) {
      case DIMENSION:
        spec = new DimensionFieldSpec();
        break;

      case METRIC:
        spec = new MetricFieldSpec();
        break;

      case TIME:
        spec = new TimeFieldSpec(column, dataType, genSpec.getTimeUnitMap().get(column));
        break;

        default:
          throw new RuntimeException("Invalid Field type.");
    }

    spec.setName(column);
    spec.setDataType(dataType);
    spec.setSingleValueField(true);

    return spec;
  }

  public static void main(String[] args) throws IOException, JSONException {
    final String[] columns = { "column1", "column2", "column3", "column4", "column5" };
    final Map<String, DataType> dataTypes = new HashMap<String, DataType>();
    final Map<String, FieldType> fieldTypes = new HashMap<String, FieldType>();
    final Map<String, TimeUnit> timeUnits = new HashMap<String, TimeUnit>();

    final Map<String, Integer> cardinality = new HashMap<String, Integer>();
    final Map<String, IntRange> range = new HashMap<String, IntRange>();

    for (final String col : columns) {
      dataTypes.put(col, DataType.INT);
      fieldTypes.put(col, FieldType.DIMENSION);
      cardinality.put(col, 1000);
    }
    final DataGeneratorSpec spec = new DataGeneratorSpec(Arrays.asList(columns), cardinality,
        range, dataTypes, fieldTypes, timeUnits, FileFormat.AVRO, "/tmp/out", true);

    final DataGenerator gen = new DataGenerator();
    gen.init(spec);
    gen.generate(1000000L, 2);
  }
}
