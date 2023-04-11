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
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
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
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement ImportData command.
 */
@SuppressWarnings("unused")
@CommandLine.Command(name = "ImportData", description = "Insert data into Pinot cluster.", mixinStandardHelpOptions =
    true)
public class ImportDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportDataCommand.class);
  private static final String SEGMENT_NAME = "segment.name";

  @CommandLine.Option(names = {"-dataFilePath"}, required = true, description = "data file path.")
  private String _dataFilePath;

  @CommandLine.Option(names = {"-format"}, required = true,
      description = "Input data format.")
  private FileFormat _format;

  @CommandLine.Option(names = {"-segmentNameGeneratorType"},
      description = "Segment name generator type, default to FIXED type.")
  private String _segmentNameGeneratorType = BatchConfigProperties.SegmentNameGeneratorType.FIXED;

  @CommandLine.Option(names = {"-table"}, required = true, description = "Table name.")
  private String _table;

  @CommandLine.Option(names = {"-controllerURI"}, description = "Pinot Controller URI.")
  private String _controllerURI = "http://localhost:9000";

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;

  @CommandLine.Option(names = {"-tempDir"},
      description = "Temporary directory used to hold data during segment creation.")
  private String _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName()).getAbsolutePath();

  @CommandLine.Option(names = {"-additionalConfigs"}, arity = "1..*", description = "Additional configs to be set.")
  private List<String> _additionalConfigs;

  private AuthProvider _authProvider;

  public ImportDataCommand setDataFilePath(String dataFilePath) {
    _dataFilePath = dataFilePath;
    return this;
  }

  public ImportDataCommand setFormat(FileFormat format) {
    _format = format;
    return this;
  }

  public String getSegmentNameGeneratorType() {
    return _segmentNameGeneratorType;
  }

  public void setSegmentNameGeneratorType(String segmentNameGeneratorType) {
    _segmentNameGeneratorType = segmentNameGeneratorType;
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

  public ImportDataCommand setUser(String user) {
    _user = user;
    return this;
  }

  public ImportDataCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public ImportDataCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  public List<String> getAdditionalConfigs() {
    return _additionalConfigs;
  }

  public ImportDataCommand setAdditionalConfigs(List<String> additionalConfigs) {
    _additionalConfigs = additionalConfigs;
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
        .format("InsertData -dataFilePath %s -format %s -table %s -controllerURI %s -user %s -password %s -tempDir %s",
            _dataFilePath, _format, _table, _controllerURI, _user, "[hidden]", _tempDir);
    if (_additionalConfigs != null) {
      results += " -additionalConfigs " + Arrays.toString(_additionalConfigs.toArray());
    }
    return results;
  }

  @Override
  public final String getName() {
    return "InsertData";
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
    Map<String, String> additionalConfigs = getAdditionalConfigs(_additionalConfigs);

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    URI dataFileURI = URI.create(_dataFilePath);
    URI parent = dataFileURI.getPath().endsWith("/") ? dataFileURI.resolve("..") : dataFileURI.resolve(".");
    spec.setInputDirURI(parent.toString());
    spec.setIncludeFileNamePattern("glob:**" + dataFileURI.getPath());
    spec.setOutputDirURI(_tempDir);
    spec.setCleanUpOutputDir(true);
    spec.setOverwriteOutput(true);
    spec.setJobType("SegmentCreationAndTarPush");
    spec.setAuthToken(makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password).getTaskToken());

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
    String inputFileScheme = dataFileURI.getScheme();
    if ((inputFileScheme != null) && (!PinotFSFactory.isSchemeSupported(inputFileScheme))) {
      pinotFSSpecs.add(getPinotFSSpec(inputFileScheme, getPinotFSClassName(inputFileScheme, additionalConfigs),
          getPinotFSConfigs(inputFileScheme, additionalConfigs)));
    }
    spec.setPinotFSSpecs(pinotFSSpecs);

    // set RecordReaderSpec
    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat(_format.name());
    recordReaderSpec.setClassName(getRecordReaderClass(_format));
    recordReaderSpec.setConfigClassName(getRecordReaderConfigClass(_format));
    recordReaderSpec.setConfigs(IngestionConfigUtils.getRecordReaderProps(additionalConfigs));
    spec.setRecordReaderSpec(recordReaderSpec);

    // set TableSpec
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(_table);
    tableSpec.setSchemaURI(ControllerRequestURLBuilder.baseUrl(_controllerURI).forTableSchemaGet(_table));
    tableSpec.setTableConfigURI(ControllerRequestURLBuilder.baseUrl(_controllerURI).forTableGet(_table));
    spec.setTableSpec(tableSpec);

    // set SegmentNameGeneratorSpec
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
    String segmentNameGeneratorType = getSegmentNameGeneratorType(additionalConfigs);
    segmentNameGeneratorSpec.setType(segmentNameGeneratorType);
    segmentNameGeneratorSpec.setConfigs(getSegmentNameGeneratorConfig(segmentNameGeneratorType, additionalConfigs));
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

  private Map<String, String> getSegmentNameGeneratorConfig(String type, Map<String, String> additionalConfigs) {
    Map<String, String> segmentNameGeneratorConfig = new HashMap<>(additionalConfigs);
    if ((BatchConfigProperties.SegmentNameGeneratorType.FIXED.equalsIgnoreCase(type)) && (!segmentNameGeneratorConfig
        .containsKey(SEGMENT_NAME))) {
      segmentNameGeneratorConfig
          .put(SEGMENT_NAME, String.format("%s_%s", _table, DigestUtils.sha256Hex(_dataFilePath)));
    }
    return segmentNameGeneratorConfig;
  }

  private String getSegmentNameGeneratorType(Map<String, String> additionalConfigs) {
    if (_segmentNameGeneratorType == null) {
      return additionalConfigs.getOrDefault(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
          BatchConfigProperties.SegmentNameGeneratorType.FIXED);
    }
    return _segmentNameGeneratorType;
  }

  private Map<String, String> getPinotFSConfigs(String scheme, Map<String, String> additionalConfigs) {
    Map<String, String> fsConfigs = new HashMap<>();
    fsConfigs.putAll(IngestionConfigUtils.getConfigMapWithPrefix(additionalConfigs, String.format("fs.%s.", scheme)));
    switch (scheme) {
      case "s3":
        fsConfigs.putIfAbsent("region", System.getProperty("AWS_REGION", "us-west-2"));
        break;
      default:
        break;
    }
    return fsConfigs;
  }

  private String getPinotFSClassName(String scheme, Map<String, String> additionalConfigs) {
    String fsClassName = additionalConfigs.get(String.format("fs.%s.className", scheme));
    if (fsClassName != null) {
      return fsClassName;
    }
    switch (scheme) {
      case "s3":
        return "org.apache.pinot.plugin.filesystem.S3PinotFS";
      default:
        throw new IllegalArgumentException("Unknown input file scheme - " + scheme);
    }
  }

  private PinotFSSpec getPinotFSSpec(String scheme, String className, Map<String, String> configs) {
    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setScheme(scheme);
    pinotFSSpec.setClassName(className);
    pinotFSSpec.setConfigs(configs);
    return pinotFSSpec;
  }

  private Map<String, String> getAdditionalConfigs(List<String> additionalConfigs) {
    if (additionalConfigs == null) {
      return Collections.emptyMap();
    }
    Map<String, String> recordReaderConfigs = new HashMap<>();
    for (String kvPair : additionalConfigs) {
      String[] splits = kvPair.split("=", 2);
      if ((splits.length == 2) && (splits[0] != null) && (splits[1] != null)) {
        recordReaderConfigs.put(splits[0], splits[1]);
      }
    }
    return recordReaderConfigs;
  }

  private String getRecordReaderConfigClass(FileFormat format) {
    switch (format) {
      case AVRO:
      case GZIPPED_AVRO:
        return "org.apache.pinot.plugin.inputformat.avro.AvroRecordReaderConfig";
      case CSV:
        return "org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig";
      case PROTO:
        return "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReaderConfig";
      case THRIFT:
        return "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig";
      case ORC:
      case JSON:
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
