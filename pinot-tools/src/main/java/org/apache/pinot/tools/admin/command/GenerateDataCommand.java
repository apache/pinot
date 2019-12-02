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
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.math.IntRange;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.data.generator.DataGenerator;
import org.apache.pinot.tools.data.generator.DataGeneratorSpec;
import org.apache.pinot.tools.data.generator.SchemaAnnotation;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement GenerateData command.
 *
 */
public class GenerateDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataCommand.class);

  @Option(name = "-numRecords", required = true, metaVar = "<int>", usage = "Number of records to generate.")
  private int _numRecords = 0;

  @Option(name = "-numFiles", required = true, metaVar = "<int>", usage = "Number of files to generate.")
  private int _numFiles = 0;

  @Option(name = "-schemaFile", required = true, metaVar = "<string>", usage = "File containing schema for data.")
  private String _schemaFile = null;

  @Option(name = "-schemaAnnotationFile", required = false, metaVar = "<string>", usage = "File containing dim/metrics for columns.")
  private String _schemaAnnFile;

  @Option(name = "-outDir", required = true, metaVar = "<string>", usage = "Directory where data would be generated.")
  private String _outDir = null;

  @Option(name = "-overwrite", required = false, usage = "Overwrite, if directory exists")
  boolean _overwrite;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

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
  public String description() {
    return "Generate random data as per the provided scema";
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: " + toString());

    if ((_numRecords < 0) || (_numFiles < 0)) {
      throw new RuntimeException("Cannot generate negative number of records/files.");
    }

    Schema schema = Schema.fromFile(new File(_schemaFile));

    List<String> columns = new LinkedList<String>();
    final HashMap<String, DataType> dataTypes = new HashMap<String, DataType>();
    final HashMap<String, FieldType> fieldTypes = new HashMap<String, FieldType>();
    final HashMap<String, TimeUnit> timeUnits = new HashMap<String, TimeUnit>();

    final HashMap<String, Integer> cardinality = new HashMap<String, Integer>();
    final HashMap<String, IntRange> range = new HashMap<String, IntRange>();

    buildCardinalityRangeMaps(_schemaAnnFile, cardinality, range);
    final DataGeneratorSpec spec =
        buildDataGeneratorSpec(schema, columns, dataTypes, fieldTypes, timeUnits, cardinality, range);

    final DataGenerator gen = new DataGenerator();
    gen.init(spec);
    gen.generate(_numRecords, _numFiles);

    return true;
  }

  private void buildCardinalityRangeMaps(String file, HashMap<String, Integer> cardinality,
      HashMap<String, IntRange> range)
      throws IOException {
    if (file == null) {
      return; // Nothing to do here.
    }

    List<SchemaAnnotation> saList = JsonUtils.fileToObject(new File(file), List.class);

    for (SchemaAnnotation sa : saList) {
      String column = sa.getColumn();

      if (sa.isRange()) {
        range.put(column, new IntRange(sa.getRangeStart(), sa.getRangeEnd()));
      } else {
        cardinality.put(column, sa.getCardinality());
      }
    }
  }

  private DataGeneratorSpec buildDataGeneratorSpec(Schema schema, List<String> columns,
      HashMap<String, DataType> dataTypes, HashMap<String, FieldType> fieldTypes, HashMap<String, TimeUnit> timeUnits,
      HashMap<String, Integer> cardinality, HashMap<String, IntRange> range) {
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

        default:
          throw new RuntimeException("Invalid field type.");
      }
    }

    return new DataGeneratorSpec(columns, cardinality, range, dataTypes, fieldTypes, timeUnits, FileFormat.AVRO,
        _outDir, _overwrite);
  }

  public static void main(String[] args)
      throws IOException {
    SchemaBuilder schemaBuilder = new SchemaBuilder();

    schemaBuilder.addSingleValueDimension("name", DataType.STRING);
    schemaBuilder.addSingleValueDimension("age", DataType.INT);
    schemaBuilder.addMetric("percent", DataType.FLOAT);
    schemaBuilder.addTime("days", TimeUnit.DAYS, DataType.LONG);

    Schema schema = schemaBuilder.build();
    System.out.println(JsonUtils.objectToPrettyString(schema));
  }
}
