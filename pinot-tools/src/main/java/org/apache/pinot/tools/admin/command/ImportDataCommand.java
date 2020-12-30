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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement ImportData command.
 */
@SuppressWarnings("unused")
public class ImportDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportDataCommand.class);
  private static final String SEGMENT_NAME = "segment.name";

  @Option(name = "-dataFilePath", required = true, metaVar = "<string>", usage = "data file path.")
  private String _dataFilePath;

  @Option(name = "-format", required = true, metaVar = "<AVRO/CSV/JSON/THRIFT/PARQUET/ORC>", usage = "Input data format.")
  private FileFormat _format;

  @Option(name = "-table", required = true, metaVar = "<string>", usage = "Table name.")
  private String _table;

  @Option(name = "-controllerURI", metaVar = "<string>", usage = "Pinot Controller URI.")
  private String _controllerURI = "http://localhost:9000";

  @Option(name = "-tempDir", metaVar = "<string>", usage = "Temporary directory used to hold data during segment creation.")
  private String _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName()).getAbsolutePath();

  @Option(name = "-extraConfigs", metaVar = "<extra configs>", handler = StringArrayOptionHandler.class, usage = "Extra configs to be set.")
  private List<String> _extraConfigs;

  @SuppressWarnings("FieldCanBeLocal")
  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public ImportDataCommand setDataFilePath(String dataFilePath) {
    _dataFilePath = dataFilePath;
    return this;
  }

  public ImportDataCommand setFormat(FileFormat format) {
    _format = format;
    return this;
  }

  public ImportDataCommand setTable(String table) {
    _table = table;
    return this;
  }

  public ImportDataCommand setControllerURI(String controllerURI) {
    _controllerURI = controllerURI;
    return this;
  }

  public ImportDataCommand setTempDir(String tempDir) {
    _tempDir = tempDir;
    return this;
  }

  public List<String> getExtraConfigs() {
    return _extraConfigs;
  }

  public ImportDataCommand setExtraConfigs(List<String> extraConfigs) {
    _extraConfigs = extraConfigs;
    return this;
  }

  public String getDataFilePath() {
    return _dataFilePath;
  }

  public FileFormat getFormat() {
    return _format;
  }

  public String getTable() {
    return _table;
  }

  public String getControllerURI() {
    return _controllerURI;
  }

  public String getTempDir() {
    return _tempDir;
  }

  @Override
  public String toString() {
    String results = String
        .format("InsertData -dataFilePath %s -format %s -table %s -controllerURI %s -tempDir %s", _dataFilePath,
            _format, _table, _controllerURI, _tempDir);
    if (_extraConfigs != null) {
      results += " -extraConfigs " + Arrays.toString(_extraConfigs.toArray());
    }
    return results;
  }

  @Override
  public final String getName() {
    return "InsertData";
  }

  @Override
  public String description() {
    return "Insert data into Pinot cluster.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute()
      throws IOException {
    LOGGER.info("Executing command: {}", toString());
    Preconditions.checkArgument(_table != null, "'table' must be specified");
    Preconditions.checkArgument(_format != null, "'format' must be specified");
    Preconditions.checkArgument(_dataFilePath != null, "'dataFilePath' must be specified");

    try {

      URI dataFileURI = URI.create(_dataFilePath);
      if ((dataFileURI.getScheme() == null)) {
        File dataFile = new File(_dataFilePath);
        Preconditions.checkArgument(dataFile.exists(), "'dataFile': '%s' doesn't exist", dataFile);
        LOGGER.info("Found data files: {} of format: {}", dataFile, _format);
      }

      initTempDir();
      IngestionJobLauncher.runIngestionJob(generateSegmentGenerationJobSpec());
      LOGGER.info("Successfully load data from {} to Pinot.", _dataFilePath);
      return true;
    } catch (Exception e) {
      throw e;
    } finally {
      FileUtils.deleteQuietly(new File(_tempDir));
    }
  }

  private void initTempDir()
      throws IOException {
    File tempDir = new File(_tempDir);
    if (tempDir.exists()) {
      LOGGER.info("Deleting the existing 'tempDir': {}", tempDir);
      FileUtils.forceDelete(tempDir);
    }
    FileUtils.forceMkdir(tempDir);
  }

  private SegmentGenerationJobSpec generateSegmentGenerationJobSpec() {
    final Map<String, String> extraConfigs = getExtraConfigs(_extraConfigs);

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    URI dataFileURI = URI.create(_dataFilePath);
    URI parent = dataFileURI.getPath().endsWith("/") ? dataFileURI.resolve("..") : dataFileURI.resolve(".");
    spec.setInputDirURI(parent.toString());
    spec.setIncludeFileNamePattern("glob:**" + dataFileURI.getPath());
    spec.setOutputDirURI(_tempDir);
    spec.setCleanUpOutputDir(true);
    spec.setOverwriteOutput(true);
    spec.setJobType("SegmentCreationAndTarPush");

    // set ExecutionFrameworkSpec
    ExecutionFrameworkSpec executionFrameworkSpec = new ExecutionFrameworkSpec();
    executionFrameworkSpec.setName("standalone");
    executionFrameworkSpec.setSegmentGenerationJobRunnerClassName(
        "org.apache.pinot.plugin.ingestion.batch.standalone.SegmentGenerationJobRunner");
    executionFrameworkSpec.setSegmentTarPushJobRunnerClassName(
        "org.apache.pinot.plugin.ingestion.batch.standalone.SegmentTarPushJobRunner");
    spec.setExecutionFrameworkSpec(executionFrameworkSpec);

    // set PinotFSSpecs
    List<PinotFSSpec> pinotFSSpecs = new ArrayList<>();
    pinotFSSpecs.add(getPinotFSSpec("file", "org.apache.pinot.spi.filesystem.LocalPinotFS", Collections.emptyMap()));
    pinotFSSpecs
        .add(getPinotFSSpec("s3", "org.apache.pinot.plugin.filesystem.S3PinotFS", getS3PinotFSConfigs(extraConfigs)));
    spec.setPinotFSSpecs(pinotFSSpecs);

    // set RecordReaderSpec
    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat(_format.name());
    recordReaderSpec.setClassName(getRecordReaderClass(_format));
    recordReaderSpec.setConfigClassName(getRecordReaderConfigClass(_format));
    recordReaderSpec.setConfigs(IngestionConfigUtils.getRecordReaderProps(extraConfigs));
    spec.setRecordReaderSpec(recordReaderSpec);

    // set TableSpec
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(_table);
    tableSpec.setSchemaURI(ControllerRequestURLBuilder.baseUrl(_controllerURI).forTableSchemaGet(_table));
    tableSpec.setTableConfigURI(ControllerRequestURLBuilder.baseUrl(_controllerURI).forTableGet(_table));
    spec.setTableSpec(tableSpec);

    // set SegmentNameGeneratorSpec
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
    segmentNameGeneratorSpec
        .setType(org.apache.pinot.spi.ingestion.batch.BatchConfigProperties.SegmentNameGeneratorType.FIXED);
    String segmentName = (extraConfigs.containsKey(SEGMENT_NAME)) ? extraConfigs.get(SEGMENT_NAME)
        : String.format("%s_%s", _table, DigestUtils.sha256Hex(_dataFilePath));
    segmentNameGeneratorSpec.setConfigs(ImmutableMap.of(SEGMENT_NAME, segmentName));
    spec.setSegmentNameGeneratorSpec(segmentNameGeneratorSpec);

    // set PinotClusterSpecs
    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(_controllerURI);
    PinotClusterSpec[] pinotClusterSpecs = new PinotClusterSpec[]{pinotClusterSpec};
    spec.setPinotClusterSpecs(pinotClusterSpecs);

    // set PushJobSpec
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(3);
    pushJobSpec.setPushRetryIntervalMillis(10000);
    spec.setPushJobSpec(pushJobSpec);

    return spec;
  }

  private Map<String, String> getS3PinotFSConfigs(Map<String, String> extraConfigs) {
    Map<String, String> s3PinotFSConfigs = new HashMap<>();
    s3PinotFSConfigs.put("region", System.getProperty("AWS_REGION", "us-west-2"));
    s3PinotFSConfigs.putAll(IngestionConfigUtils.getConfigMapWithPrefix(extraConfigs,
        BatchConfigProperties.INPUT_FS_PROP_PREFIX + IngestionConfigUtils.DOT_SEPARATOR));
    return s3PinotFSConfigs;
  }

  private PinotFSSpec getPinotFSSpec(String scheme, String className, Map<String, String> configs) {
    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setScheme(scheme);
    pinotFSSpec.setClassName(className);
    pinotFSSpec.setConfigs(configs);
    return pinotFSSpec;
  }

  private Map<String, String> getExtraConfigs(List<String> extraConfigs) {
    if (extraConfigs == null) {
      return Collections.emptyMap();
    }
    Map<String, String> recordReaderConfigs = new HashMap<>();
    for (String kvPair : extraConfigs) {
      String[] splits = kvPair.split("=", 2);
      if ((splits.length == 2) && (splits[0] != null) && (splits[1] != null)) {
        recordReaderConfigs.put(splits[0], splits[1]);
      }
    }
    return recordReaderConfigs;
  }

  private String getRecordReaderConfigClass(FileFormat format) {
    switch (format) {
      case CSV:
        return "org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig";
      case PROTO:
        return "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReaderConfig";
      case THRIFT:
        return "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig";
      case ORC:
      case JSON:
      case AVRO:
      case GZIPPED_AVRO:
      case PARQUET:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported file format - " + format);
    }
  }

  private String getRecordReaderClass(FileFormat format) {
    switch (format) {
      case CSV:
        return "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader";
      case ORC:
        return "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader";
      case JSON:
        return "org.apache.pinot.plugin.inputformat.json.JSONRecordReader";
      case AVRO:
      case GZIPPED_AVRO:
        return "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader";
      case PARQUET:
        return "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader";
      case PROTO:
        return "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReader";
      case THRIFT:
        return "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader";
      default:
        throw new IllegalArgumentException("Unsupported file format - " + format);
    }
  }
}
