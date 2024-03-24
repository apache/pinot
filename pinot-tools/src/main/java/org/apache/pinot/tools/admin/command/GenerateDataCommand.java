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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Range;
import org.apache.pinot.controller.recommender.data.DataGenerationHelpers;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.apache.pinot.controller.recommender.data.generator.DataGeneratorSpec;
import org.apache.pinot.controller.recommender.data.generator.SchemaAnnotation;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement GenerateData command.
 *
 */
@CommandLine.Command(name = "GenerateData")
public class GenerateDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataCommand.class);

  private final static String FORMAT_AVRO = "avro";
  private final static String FORMAT_CSV = "csv";
  private final static String FORMAT_JSON = "json";

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

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  @CommandLine.Option(names = {"-format"}, required = false, help = true,
      description = "Output format ('AVRO' or 'CSV' or 'JSON').")
  private String _format = FORMAT_AVRO;

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
    return "Generate random data as per the provided schema";
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: " + toString());

    if ((_numRecords < 0) || (_numFiles < 0)) {
      throw new RuntimeException("Cannot generate negative number of records/files.");
    }

    Schema schema = Schema.fromFile(new File(_schemaFile));
    final DataGeneratorSpec spec =
        DataGenerationHelpers.buildDataGeneratorSpec(schema);
    buildCardinalityRangeMaps(_schemaAnnFile, spec);


    final DataGenerator gen = new DataGenerator();
    gen.init(spec);

    if (FORMAT_AVRO.equalsIgnoreCase(_format)) {
      DataGenerationHelpers.generateAvro(gen, _numRecords, _numFiles, _outDir, _overwrite);
    } else if (FORMAT_CSV.equalsIgnoreCase(_format)) {
      DataGenerationHelpers.generateCsv(gen, _numRecords, _numFiles, _outDir, _overwrite);
    } else if (FORMAT_JSON.equalsIgnoreCase(_format)) {
      DataGenerationHelpers.generateJson(gen, _numRecords, _numFiles, _outDir, _overwrite);
    } else {
      throw new IllegalArgumentException(String.format("Invalid output format '%s'", _format));
    }

    return true;
  }

  private void buildCardinalityRangeMaps(String file, DataGeneratorSpec spec)
      throws IOException {
    if (file == null) {
      return; // Nothing to do here.
    }

    List<SchemaAnnotation> saList = JsonUtils.fileToList(new File(file), SchemaAnnotation.class);

    for (SchemaAnnotation sa : saList) {
      String column = sa.getColumn();

      if (sa.isRange()) {
        spec.getRangeMap().put(column, Range.of(sa.getRangeStart(), sa.getRangeEnd()));
      } else if (sa.getPattern() != null) {
        spec.getPatternMap().put(column, sa.getPattern());
      } else {
        spec.getCardinalityMap().put(column, sa.getCardinality());
      }
    }
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
