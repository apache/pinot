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
package org.apache.pinot.hadoop.job.mappers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentCreationJob;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReaderConfig;
import org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  protected static final String LOCAL_TEMP_DIR = "pinot_hadoop_tmp";
  protected static final String PROGRESS_REPORTER_THREAD_NAME = "pinot-hadoop-progress-reporter";
  protected static final long PROGRESS_REPORTER_JOIN_WAIT_TIME_MS = 5_000L;

  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  protected Configuration _jobConf;
  protected String _rawTableName;
  protected Schema _schema;
  protected SegmentNameGenerator _segmentNameGenerator;
  protected boolean _useRelativePath = false;
  protected boolean _failIfSchemaMismatch = false;

  // Optional
  protected TableConfig _tableConfig;
  protected String _recordReaderPath;
  protected Path _readerConfigFile;

  // HDFS segment tar directory
  protected Path _hdfsSegmentTarDir;

  // Temporary local directories
  protected File _localStagingDir;
  protected File _localInputDir;
  protected File _localSegmentDir;
  protected File _localSegmentTarDir;

  // Counter for detecting schema mismatch
  private int _dataTypeMismatch = 0;
  private int _singleValueMultiValueFieldMismatch = 0;
  private int _multiValueStructureMismatch = 0;
  private int _missingPinotColumn = 0;

  /**
   * Generate a relative output directory path when `useRelativePath` flag is on.
   * This method will compute the relative path based on `inputFile` and `baseInputDir`,
   * then apply only the directory part of relative path to `outputDir`.
   * E.g.
   *    baseInputDir = "/path/to/input"
   *    inputFile = "/path/to/input/a/b/c/d.avro"
   *    outputDir = "/path/to/output"
   *    getRelativeOutputPath(baseInputDir, inputFile, outputDir) = /path/to/output/a/b/c
   */
  protected static Path getRelativeOutputPath(URI baseInputDir, URI inputFile, Path outputDir) {
    URI relativePath = baseInputDir.relativize(inputFile);
    Preconditions.checkState(relativePath.getPath().length() > 0 && !relativePath.equals(inputFile),
        "Unable to extract out the relative path based on base input path: " + baseInputDir);
    return new Path(outputDir, relativePath.getPath()).getParent();
  }

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    _jobConf = context.getConfiguration();
    logConfigurations();

    _useRelativePath = _jobConf.getBoolean(JobConfigConstants.USE_RELATIVE_PATH, false);
    _rawTableName = _jobConf.get(JobConfigConstants.SEGMENT_TABLE_NAME);
    _schema = Schema.fromString(_jobConf.get(JobConfigConstants.SCHEMA));

    // Optional.
    // Once we move to dateTimeFieldSpec, check that table config (w/ valid timeColumnName) is provided if multiple dateTimeFieldSpecs are configured
    String tableConfigString = _jobConf.get(JobConfigConstants.TABLE_CONFIG);
    if (tableConfigString != null) {
      _tableConfig = JsonUtils.stringToObject(tableConfigString, TableConfig.class);
    }
    String readerConfigFile = _jobConf.get(JobConfigConstants.PATH_TO_READER_CONFIG);
    if (readerConfigFile != null) {
      _readerConfigFile = new Path(readerConfigFile);
    }
    _recordReaderPath = _jobConf.get(JobConfigConstants.RECORD_READER_PATH);
    Preconditions.checkNotNull(_tableConfig);

    setFlagForSchemaMismatch();

    // Set up segment name generator
    String segmentNameGeneratorType =
        _jobConf.get(JobConfigConstants.SEGMENT_NAME_GENERATOR_TYPE, JobConfigConstants.DEFAULT_SEGMENT_NAME_GENERATOR);
    switch (segmentNameGeneratorType) {
      case JobConfigConstants.SIMPLE_SEGMENT_NAME_GENERATOR:
        _segmentNameGenerator =
            new SimpleSegmentNameGenerator(_rawTableName, _jobConf.get(JobConfigConstants.SEGMENT_NAME_POSTFIX));
        break;
      case JobConfigConstants.NORMALIZED_DATE_SEGMENT_NAME_GENERATOR:
        Preconditions.checkState(_tableConfig != null,
            "In order to use NormalizedDateSegmentNameGenerator, table config must be provided");
        SegmentsValidationAndRetentionConfig validationConfig = _tableConfig.getValidationConfig();
        DateTimeFormatSpec dateTimeFormatSpec = null;
        String timeColumnName = _tableConfig.getValidationConfig().getTimeColumnName();

        if (timeColumnName != null) {
          DateTimeFieldSpec dateTimeFieldSpec = _schema.getSpecForTimeColumn(timeColumnName);
          if (dateTimeFieldSpec != null) {
            dateTimeFormatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
          }
        }
        _segmentNameGenerator =
            new NormalizedDateSegmentNameGenerator(_rawTableName, _jobConf.get(JobConfigConstants.SEGMENT_NAME_PREFIX),
                _jobConf.getBoolean(JobConfigConstants.EXCLUDE_SEQUENCE_ID, false),
                IngestionConfigUtils.getBatchSegmentIngestionType(_tableConfig),
                IngestionConfigUtils.getBatchSegmentIngestionFrequency(_tableConfig), dateTimeFormatSpec);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported segment name generator type: " + segmentNameGeneratorType);
    }

    // Working directories
    _hdfsSegmentTarDir = new Path(FileOutputFormat.getWorkOutputPath(context), JobConfigConstants.SEGMENT_TAR_DIR);
    _localStagingDir = new File(LOCAL_TEMP_DIR);
    _localInputDir = new File(_localStagingDir, "inputData");
    _localSegmentDir = new File(_localStagingDir, "segments");
    _localSegmentTarDir = new File(_localStagingDir, JobConfigConstants.SEGMENT_TAR_DIR);

    if (_localStagingDir.exists()) {
      _logger.warn("Deleting existing file: {}", _localStagingDir);
      FileUtils.forceDelete(_localStagingDir);
    }
    _logger.info("Making local temporary directories: {}, {}, {}", _localStagingDir, _localInputDir,
        _localSegmentTarDir);
    Preconditions.checkState(_localStagingDir.mkdirs());
    Preconditions.checkState(_localInputDir.mkdir());
    Preconditions.checkState(_localSegmentDir.mkdir());
    Preconditions.checkState(_localSegmentTarDir.mkdir());

    _logger.info("*********************************************************************");
    _logger.info("Raw Table Name: {}", _rawTableName);
    _logger.info("Schema: {}", _schema);
    _logger.info("Segment Name Generator: {}", _segmentNameGenerator);
    _logger.info("Table Config: {}", _tableConfig);
    _logger.info("Reader Config File: {}", _readerConfigFile);
    _logger.info("*********************************************************************");
    _logger.info("HDFS Segment Tar Directory: {}", _hdfsSegmentTarDir);
    _logger.info("Local Staging Directory: {}", _localStagingDir);
    _logger.info("Local Input Directory: {}", _localInputDir);
    _logger.info("Local Segment Tar Directory: {}", _localSegmentTarDir);
    _logger.info("*********************************************************************");
  }

  protected void logConfigurations() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append('{');
    boolean firstEntry = true;
    for (Map.Entry<String, String> entry : _jobConf) {
      if (!firstEntry) {
        stringBuilder.append(", ");
      } else {
        firstEntry = false;
      }

      stringBuilder.append(entry.getKey());
      stringBuilder.append('=');
      stringBuilder.append(entry.getValue());
    }
    stringBuilder.append('}');

    _logger.info("*********************************************************************");
    _logger.info("Job Configurations: {}", stringBuilder.toString());
    _logger.info("*********************************************************************");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] splits = StringUtils.split(value.toString(), ' ');
    Preconditions.checkState(splits.length == 2, "Illegal input value: {}", value);

    Path hdfsInputFile = new Path(splits[0]);
    int sequenceId = Integer.parseInt(splits[1]);
    _logger.info("Generating segment with HDFS input file: {}, sequence id: {}", hdfsInputFile, sequenceId);

    String inputFileName = hdfsInputFile.getName();
    File localInputFile = new File(_localInputDir, inputFileName);
    _logger.info("Copying input file from: {} to: {}", hdfsInputFile, localInputFile);
    FileSystem.get(hdfsInputFile.toUri(), _jobConf).copyToLocalFile(hdfsInputFile,
        new Path(localInputFile.getAbsolutePath()));

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, _schema);
    segmentGeneratorConfig.setTableName(_rawTableName);
    segmentGeneratorConfig.setInputFilePath(localInputFile.getPath());
    segmentGeneratorConfig.setOutDir(_localSegmentDir.getPath());
    segmentGeneratorConfig.setSegmentNameGenerator(_segmentNameGenerator);
    segmentGeneratorConfig.setSequenceId(sequenceId);
    if (_recordReaderPath != null) {
      segmentGeneratorConfig.setRecordReaderPath(_recordReaderPath);
      segmentGeneratorConfig.setFormat(FileFormat.OTHER);
    } else {
      FileFormat fileFormat = getFileFormat(inputFileName);
      segmentGeneratorConfig.setFormat(fileFormat);
      segmentGeneratorConfig.setReaderConfig(getReaderConfig(fileFormat));
    }
    segmentGeneratorConfig.setOnHeap(true);
    // Enable failing the job when meeting empty record to early detect potential issue from upstream.
    // This is useful since releasing the constraint in offline job could allow unexpected issues appear without people's notice.
    segmentGeneratorConfig.setFailOnEmptySegment(true);

    addAdditionalSegmentGeneratorConfigs(segmentGeneratorConfig, hdfsInputFile, sequenceId);

    _logger.info("Start creating segment with sequence id: {}", sequenceId);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();

    // Start a thread that reports progress every minute during segment generation to prevent job getting killed
    Thread progressReporterThread = new Thread(getProgressReporter(context));
    progressReporterThread.setName(PROGRESS_REPORTER_THREAD_NAME);
    progressReporterThread.start();
    try {
      driver.init(segmentGeneratorConfig);
      validateSchema(driver.getIngestionSchemaValidator());
      driver.build();
    } catch (Exception e) {
      _logger.error("Caught exception while creating segment with HDFS input file: {}, sequence id: {}", hdfsInputFile,
          sequenceId, e);
      throw new RuntimeException(e);
    } finally {
      progressReporterThread.interrupt();
      progressReporterThread.join(PROGRESS_REPORTER_JOIN_WAIT_TIME_MS);
      if (progressReporterThread.isAlive()) {
        _logger.error("Failed to interrupt progress reporter thread: {}", progressReporterThread);
      }
    }
    String segmentName = driver.getSegmentName();
    _logger.info("Finish creating segment: {} with sequence id: {}", segmentName, sequenceId);

    File localSegmentDir = new File(_localSegmentDir, segmentName);
    String segmentTarFileName = segmentName + JobConfigConstants.TAR_GZ_FILE_EXT;
    File localSegmentTarFile = new File(_localSegmentTarDir, segmentTarFileName);
    _logger.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
    TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);

    long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
    long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
    _logger.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
        DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));

    Path hdfsSegmentTarFile = new Path(_hdfsSegmentTarDir, segmentTarFileName);
    if (_useRelativePath) {
      Path relativeOutputPath = getRelativeOutputPath(new Path(_jobConf.get(JobConfigConstants.PATH_TO_INPUT)).toUri(),
          hdfsInputFile.toUri(), _hdfsSegmentTarDir);
      hdfsSegmentTarFile = new Path(relativeOutputPath, segmentTarFileName);
    }
    _logger.info("Copying segment tar file from: {} to: {}", localSegmentTarFile, hdfsSegmentTarFile);
    FileSystem.get(hdfsSegmentTarFile.toUri(), _jobConf).copyFromLocalFile(true, true,
        new Path(localSegmentTarFile.getAbsolutePath()), hdfsSegmentTarFile);

    context.write(new LongWritable(sequenceId), new Text(segmentTarFileName));
    _logger.info("Finish generating segment: {} with HDFS input file: {}, sequence id: {}", segmentName, hdfsInputFile,
        sequenceId);
  }

  protected FileFormat getFileFormat(String fileName) {
    if (fileName.endsWith(".avro")) {
      return FileFormat.AVRO;
    }
    if (fileName.endsWith(".csv")) {
      return FileFormat.CSV;
    }
    if (fileName.endsWith(".json")) {
      return FileFormat.JSON;
    }
    if (fileName.endsWith(".thrift")) {
      return FileFormat.THRIFT;
    }
    throw new IllegalArgumentException("Unsupported file format: {}" + fileName);
  }

  @Nullable
  protected RecordReaderConfig getReaderConfig(FileFormat fileFormat) throws IOException {
    if (_readerConfigFile != null) {
      if (fileFormat == FileFormat.CSV) {
        try (InputStream inputStream = FileSystem.get(_readerConfigFile.toUri(), _jobConf).open(_readerConfigFile)) {
          CSVRecordReaderConfig readerConfig = JsonUtils.inputStreamToObject(inputStream, CSVRecordReaderConfig.class);
          _logger.info("Using CSV record reader config: {}", readerConfig);
          return readerConfig;
        }
      }
      if (fileFormat == FileFormat.THRIFT) {
        try (InputStream inputStream = FileSystem.get(_readerConfigFile.toUri(), _jobConf).open(_readerConfigFile)) {
          ThriftRecordReaderConfig readerConfig =
              JsonUtils.inputStreamToObject(inputStream, ThriftRecordReaderConfig.class);
          _logger.info("Using Thrift record reader config: {}", readerConfig);
          return readerConfig;
        }
      }

      if (fileFormat == FileFormat.PROTO) {
        try (InputStream inputStream = FileSystem.get(_readerConfigFile.toUri(), _jobConf).open(_readerConfigFile)) {
          ProtoBufRecordReaderConfig readerConfig =
              JsonUtils.inputStreamToObject(inputStream, ProtoBufRecordReaderConfig.class);
          _logger.info("Using Protocol Buffer record reader config: {}", readerConfig);
          return readerConfig;
        }
      }
    }
    return null;
  }

  protected Runnable getProgressReporter(Context context) {
    return new ProgressReporter(context);
  }

  /**
   * Can be overridden to set additional segment generator configs.
   */
  @SuppressWarnings("unused")
  protected void addAdditionalSegmentGeneratorConfigs(SegmentGeneratorConfig segmentGeneratorConfig, Path hdfsInputFile,
      int sequenceId) {
  }

  private void setFlagForSchemaMismatch() {
    String failJobMsg = "Set to fail the job if schemas mismatch.";
    String notFailJobMsg = "Set NOT to fail the job if schemas mismatch.";
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      _logger.info(notFailJobMsg);
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey(InternalConfigConstants.FAIL_ON_SCHEMA_MISMATCH)) {
      _failIfSchemaMismatch =
          Boolean.parseBoolean(customConfigsMap.get(InternalConfigConstants.FAIL_ON_SCHEMA_MISMATCH));
    }
    _logger.info(_failIfSchemaMismatch ? failJobMsg : notFailJobMsg);
  }

  private void validateSchema(IngestionSchemaValidator ingestionSchemaValidator) {
    if (ingestionSchemaValidator == null) {
      return;
    }
    if (ingestionSchemaValidator.getDataTypeMismatchResult().isMismatchDetected()) {
      _dataTypeMismatch++;
      _logger.warn(ingestionSchemaValidator.getDataTypeMismatchResult().getMismatchReason());
    }
    if (ingestionSchemaValidator.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected()) {
      _singleValueMultiValueFieldMismatch++;
      ingestionSchemaValidator.getSingleValueMultiValueFieldMismatchResult().getMismatchReason();
    }
    if (ingestionSchemaValidator.getMultiValueStructureMismatchResult().isMismatchDetected()) {
      _multiValueStructureMismatch++;
      ingestionSchemaValidator.getMultiValueStructureMismatchResult().getMismatchReason();
    }
    if (ingestionSchemaValidator.getMissingPinotColumnResult().isMismatchDetected()) {
      _missingPinotColumn++;
      ingestionSchemaValidator.getMissingPinotColumnResult().getMismatchReason();
    }

    if (isSchemaMismatch() && _failIfSchemaMismatch) {
      throw new RuntimeException(
          "Schema mismatch detected. Forcing to fail the job. Please checking log message above.");
    }
  }

  private boolean isSchemaMismatch() {
    return _dataTypeMismatch + _singleValueMultiValueFieldMismatch + _multiValueStructureMismatch
        + _missingPinotColumn != 0;
  }

  @Override
  public void cleanup(Context context) {
    context.getCounter(SegmentCreationJob.SchemaMisMatchCounter.DATA_TYPE_MISMATCH).increment(_dataTypeMismatch);
    context.getCounter(SegmentCreationJob.SchemaMisMatchCounter.SINGLE_VALUE_MULTI_VALUE_FIELD_MISMATCH)
        .increment(_singleValueMultiValueFieldMismatch);
    context.getCounter(SegmentCreationJob.SchemaMisMatchCounter.MULTI_VALUE_FIELD_STRUCTURE_MISMATCH)
        .increment(_multiValueStructureMismatch);
    context.getCounter(SegmentCreationJob.SchemaMisMatchCounter.MISSING_PINOT_COLUMN).increment(_missingPinotColumn);
    _logger.info("Deleting local temporary directory: {}", _localStagingDir);
    FileUtils.deleteQuietly(_localStagingDir);
  }

  private static class ProgressReporter implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressReporter.class);
    private static final long PROGRESS_REPORTER_INTERVAL_MS = 60_000L;

    private final Context _context;

    ProgressReporter(Context context) {
      _context = context;
    }

    @Override
    public void run() {
      LOGGER.info("Starting progress reporter thread: {}", Thread.currentThread());
      while (true) {
        try {
          Thread.sleep(PROGRESS_REPORTER_INTERVAL_MS);
          LOGGER.info("============== Reporting progress ==============");
          _context.progress();
        } catch (InterruptedException e) {
          LOGGER.info("Progress reporter thread: {} interrupted", Thread.currentThread());
          return;
        }
      }
    }
  }
}
