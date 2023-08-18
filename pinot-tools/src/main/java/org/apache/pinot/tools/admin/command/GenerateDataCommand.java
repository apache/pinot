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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.math.IntRange;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.apache.pinot.controller.recommender.data.generator.DataGeneratorSpec;
import org.apache.pinot.controller.recommender.data.generator.SchemaAnnotation;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement GenerateData command.
 *
 */
@CommandLine.Command(name = "GenerateData", description = "Generate random data as per the provided schema",
    mixinStandardHelpOptions = true)
public class GenerateDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataCommand.class);

  private final static String FORMAT_AVRO = "avro";
  private final static String FORMAT_CSV = "csv";

  @CommandLine.Option(names = {"-numRecords"}, required = true, description = "Number of records to generate.")
  private int _numRecords = 0;

  @CommandLine.Option(names = {"-numFiles"}, required = true, description = "Number of files to generate.")
  private int _numFiles = 0;

  @CommandLine.Option(names = {"-schemaFile"}, required = true, description = "File containing schema for data.")
  private String _schemaFile = null;

  @CommandLine.Option(names = {"-schemaAnnotationFile"}, required = false,
      description = "File containing dim/metrics for columns.")
  private String _schemaAnnFile;

  @CommandLine.Option(names = {"-outDir"}, required = true, description = "Directory where data would be generated.")
  private String _outDir = null;

  @CommandLine.Option(names = {"-overwrite"}, required = false, description = "Overwrite, if directory exists")
  boolean _overwrite;

  @CommandLine.Option(names = {"-format"}, required = false, help = true,
      description = "Output format ('AVRO' or 'CSV').")
  private String _format = FORMAT_AVRO;

  public void init(int numRecords, int numFiles, String schemaFile, String outDir) {
    _numRecords = numRecords;
    _numFiles = numFiles;

    _schemaFile = schemaFile;
    _outDir = outDir;
  }

  @Override
  public String toString() {
    return ("GenerateData -numRecords " + _numRecords + " -numFiles " + " " + _numFiles + " -schemaFile " + _schemaFile
        + " -outDir " + _outDir + " -schemaAnnotationFile " + _schemaAnnFile);
  }

  @Override
  public String getName() {
    return "GenerateData";
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: " + toString());

    if ((_numRecords < 0) || (_numFiles < 0)) {
      throw new RuntimeException("Cannot generate negative number of records/files.");
    }

    Schema schema = Schema.fromFile(new File(_schemaFile));

    List<String> columns = new LinkedList<>();
    final HashMap<String, DataType> dataTypes = new HashMap<>();
    final HashMap<String, FieldType> fieldTypes = new HashMap<>();
    final HashMap<String, TimeUnit> timeUnits = new HashMap<>();

    final HashMap<String, Integer> cardinality = new HashMap<>();
    final HashMap<String, IntRange> range = new HashMap<>();
    final HashMap<String, Map<String, Object>> pattern = new HashMap<>();
    final HashMap<String, Double> mvCountMap = new HashMap<>();
    final HashMap<String, Integer> lengthMap = new HashMap<>();

    buildCardinalityRangeMaps(_schemaAnnFile, cardinality, range, pattern);

    final DataGeneratorSpec spec =
        buildDataGeneratorSpec(schema, columns, dataTypes, fieldTypes, timeUnits, cardinality, range, pattern,
            mvCountMap, lengthMap);

    final DataGenerator gen = new DataGenerator();
    gen.init(spec);

    if (FORMAT_AVRO.equalsIgnoreCase(_format)) {
      gen.generateAvro(_numRecords, _numFiles);
    } else if (FORMAT_CSV.equalsIgnoreCase(_format)) {
      gen.generateCsv(_numRecords, _numFiles);
    } else {
      throw new IllegalArgumentException(String.format("Invalid output format '%s'", _format));
    }

    return true;
  }

  private void buildCardinalityRangeMaps(String file, HashMap<String, Integer> cardinality,
      HashMap<String, IntRange> range, Map<String, Map<String, Object>> pattern)
      throws IOException {
    if (file == null) {
      return; // Nothing to do here.
    }

    List<SchemaAnnotation> saList = JsonUtils.fileToList(new File(file), SchemaAnnotation.class);

    for (SchemaAnnotation sa : saList) {
      String column = sa.getColumn();

      if (sa.isRange()) {
        range.put(column, new IntRange(sa.getRangeStart(), sa.getRangeEnd()));
      } else if (sa.getPattern() != null) {
        pattern.put(column, sa.getPattern());
      } else {
        cardinality.put(column, sa.getCardinality());
      }
    }
  }

  private DataGeneratorSpec buildDataGeneratorSpec(Schema schema, List<String> columns,
      HashMap<String, DataType> dataTypes, HashMap<String, FieldType> fieldTypes, HashMap<String, TimeUnit> timeUnits,
      HashMap<String, Integer> cardinality, HashMap<String, IntRange> range,
      HashMap<String, Map<String, Object>> pattern, Map<String, Double> mvCountMap, Map<String, Integer> lengthMap) {
    for (final FieldSpec fs : schema.getAllFieldSpecs()) {
      String col = fs.getName();

      columns.add(col);
      dataTypes.put(col, fs.getDataType());
      fieldTypes.put(col, fs.getFieldType());

      switch (fs.getFieldType()) {
        case DIMENSION:
          if (cardinality.get(col) == null) {
            cardinality.put(col, 1000);
          }
          break;

        case METRIC:
          if (!range.containsKey(col)) {
            range.put(col, new IntRange(1, 1000));
          }
          break;

        case TIME:
          if (!range.containsKey(col)) {
            range.put(col, new IntRange(1, 1000));
          }
          TimeFieldSpec tfs = (TimeFieldSpec) fs;
          timeUnits.put(col, tfs.getIncomingGranularitySpec().getTimeType());
          break;

        // forward compatibility with pattern generator
        case DATE_TIME:
        case COMPLEX:
          break;

        default:
          throw new RuntimeException("Invalid field type.");
      }
    }

    return new DataGeneratorSpec(columns, cardinality, range, pattern, mvCountMap, lengthMap, dataTypes, fieldTypes,
        timeUnits, FileFormat.AVRO, _outDir, _overwrite);
  }

  public static void main(String[] args)
      throws IOException {
    SchemaBuilder schemaBuilder = new SchemaBuilder();

    schemaBuilder.addSingleValueDimension("name", DataType.STRING);
    schemaBuilder.addSingleValueDimension("age", DataType.INT);
    schemaBuilder.addMetric("percent", DataType.FLOAT);
    schemaBuilder.addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "days"), null);

    Schema schema = schemaBuilder.build();
    System.out.println(JsonUtils.objectToPrettyString(schema));
  }
}
