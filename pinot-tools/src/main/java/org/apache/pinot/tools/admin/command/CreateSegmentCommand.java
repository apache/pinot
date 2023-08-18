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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement CreateSegment command.
 */
@SuppressWarnings("unused")
@CommandLine.Command(name = "CreateSegment", description = "Create pinot segments from the provided data files.",
    mixinStandardHelpOptions = true)
public class CreateSegmentCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);

  @CommandLine.Option(names = {"-dataDir"}, description = "Directory containing the data.")
  private String _dataDir;

  @CommandLine.Option(names = {"-format"}, description = "Input data format.")
  private FileFormat _format;

  @CommandLine.Option(names = {"-outDir"}, description = "Name of output directory.")
  private String _outDir;

  @CommandLine.Option(names = {"-overwrite"}, description = "Overwrite existing output directory.")
  private boolean _overwrite = false;

  @CommandLine.Option(names = {"-tableConfigFile"}, description = "File containing table config for data.")
  private String _tableConfigFile;

  @CommandLine.Option(names = {"-schemaFile"}, description = "File containing schema for data.")
  private String _schemaFile;

  @CommandLine.Option(names = {"-readerConfigFile"}, description = "Config file for record reader.")
  private String _readerConfigFile;

  @CommandLine.Option(names = {"-retry"},
      description = "Number of retries if encountered any segment creation failure, default is 0.")
  private int _retry = 0;

  @CommandLine.Option(names = {"-failOnEmptySegment"},
      description = "Option to fail the segment creation if output is an empty segment.")
  private boolean _failOnEmptySegment = false;

  @CommandLine.Option(names = {"-postCreationVerification"},
      description = "Verify segment data file after segment creation. Please ensure you have enough local disk to"
          + " hold data for verification")
  private boolean _postCreationVerification = false;

  @CommandLine.Option(names = {"-numThreads"}, description = "Parallelism while generating segments, default is 1.")
  private int _numThreads = 1;

  public CreateSegmentCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  public CreateSegmentCommand setFormat(FileFormat format) {
    _format = format;
    return this;
  }

  public CreateSegmentCommand setOutDir(String outDir) {
    _outDir = outDir;
    return this;
  }

  public CreateSegmentCommand setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
    return this;
  }

  public CreateSegmentCommand setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
    return this;
  }

  public CreateSegmentCommand setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
    return this;
  }

  public CreateSegmentCommand setReaderConfigFile(String readerConfigFile) {
    _readerConfigFile = readerConfigFile;
    return this;
  }

  public CreateSegmentCommand setRetry(int retry) {
    _retry = retry;
    return this;
  }

  public CreateSegmentCommand setFailOnEmptySegment(boolean failOnEmptySegment) {
    _failOnEmptySegment = failOnEmptySegment;
    return this;
  }

  public CreateSegmentCommand setPostCreationVerification(boolean postCreationVerification) {
    _postCreationVerification = postCreationVerification;
    return this;
  }

  public CreateSegmentCommand setNumThreads(int numThreads) {
    _numThreads = numThreads;
    return this;
  }

  @Override
  public String toString() {
    return String.format(
        "CreateSegment -dataDir %s -format %s -outDir %s -overwrite %s -tableConfigFile %s -schemaFile %s "
            + "-readerConfigFile %s -retry %d -failOnEmptySegment %s -postCreationVerification %s -numThreads %d",
        _dataDir, _format, _outDir, _overwrite, _tableConfigFile, _schemaFile, _readerConfigFile, _retry,
        _failOnEmptySegment, _postCreationVerification, _numThreads);
  }

  @Override
  public final String getName() {
    return "CreateSegment";
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: {}", toString());

    Preconditions.checkArgument(_dataDir != null, "'dataDir' must be specified");
    File dataDir = new File(_dataDir);
    Preconditions.checkArgument(dataDir.isDirectory(), "'dataDir': '%s' is not a directory", dataDir);

    // Filter out all input data files
    Preconditions.checkArgument(_format != null, "'format' must be specified");
    List<String> dataFiles = getDataFiles(dataDir);
    Preconditions
        .checkState(!dataFiles.isEmpty(), "Failed to find any data file of format: %s under directory: %s", _format,
            dataDir);
    LOGGER.info("Found data files: {} of format: {} under directory: {}", dataFiles, _format, dataDir);

    Preconditions.checkArgument(_outDir != null, "'outDir' must be specified");
    File outDir = new File(_outDir);
    if (_overwrite) {
      if (outDir.exists()) {
        LOGGER.info("Deleting the existing 'outDir': {}", outDir);
        FileUtils.forceDelete(outDir);
      }
    }
    FileUtils.forceMkdir(outDir);

    Preconditions.checkArgument(_tableConfigFile != null, "'tableConfigFile' must be specified");
    TableConfig tableConfig;
    try {
      tableConfig = JsonUtils.fileToObject(new File(_tableConfigFile), TableConfig.class);
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while reading table config from file: " + _tableConfigFile, e);
    }
    LOGGER.info("Using table config: {}", tableConfig.toJsonString());
    String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());

    Preconditions.checkArgument(_schemaFile != null, "'schemaFile' must be specified");
    Schema schema;
    try {
      schema = JsonUtils.fileToObject(new File(_schemaFile), Schema.class);
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while reading schema from file: " + _schemaFile, e);
    }
    LOGGER.info("Using schema: {}", schema.toSingleLineJsonString());

    RecordReaderConfig recordReaderConfig;
    if (_readerConfigFile != null) {
      try {
        recordReaderConfig = RecordReaderFactory.getRecordReaderConfig(_format, _readerConfigFile);
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Caught exception while reading %s record reader config from file: %s", _format,
                _readerConfigFile), e);
      }
      LOGGER.info("Using {} record reader config: {}", _format, recordReaderConfig);
    } else {
      recordReaderConfig = null;
    }

    ExecutorService executorService = Executors.newFixedThreadPool(_numThreads);
    int numDataFiles = dataFiles.size();
    Future[] futures = new Future[numDataFiles];
    for (int i = 0; i < numDataFiles; i++) {
      int sequenceId = i;
      futures[sequenceId] = executorService.submit(() -> {
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setInputFilePath(dataFiles.get(sequenceId));
        segmentGeneratorConfig.setFormat(_format);
        segmentGeneratorConfig.setOutDir(outDir.getPath());
        segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
        segmentGeneratorConfig.setTableName(rawTableName);
        segmentGeneratorConfig.setSequenceId(sequenceId);
        segmentGeneratorConfig.setFailOnEmptySegment(_failOnEmptySegment);
        for (int j = 0; j <= _retry; j++) {
          try {
            SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
            driver.init(segmentGeneratorConfig);
            driver.build();
            String segmentName = driver.getSegmentName();
            File indexDir = new File(outDir, segmentName);
            LOGGER.info("Successfully created segment: {} at directory: {}", segmentName, indexDir);
            if (_postCreationVerification) {
              LOGGER.info("Verifying the segment by loading it");
              ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
              LOGGER.info("Successfully loaded segment: {} of size: {} bytes", segmentName,
                  segment.getSegmentSizeBytes());
              segment.destroy();
              break;
            }
          } catch (Exception e) {
            if (j < _retry) {
              LOGGER.warn("Caught exception while creating/verifying segment, will retry", e);
            } else {
              throw new RuntimeException(
                  "Caught exception while generating segment from file: " + dataFiles.get(sequenceId), e);
            }
          }
        }
        return null;
      });
    }
    executorService.shutdown();
    for (Future future : futures) {
      future.get();
    }
    LOGGER.info("Successfully created {} segments from data files: {}", numDataFiles, dataFiles);
    return true;
  }

  private List<String> getDataFiles(File dataDir) {
    List<String> dataFiles = new ArrayList<>();
    //noinspection ConstantConditions
    getDataFilesHelper(dataDir.listFiles(), dataFiles);
    return dataFiles;
  }

  private void getDataFilesHelper(File[] files, List<String> dataFiles) {
    for (File file : files) {
      if (file.isDirectory()) {
        //noinspection ConstantConditions
        getDataFilesHelper(file.listFiles(), dataFiles);
      } else {
        if (isDataFile(file.getName())) {
          dataFiles.add(file.getPath());
        }
      }
    }
  }

  private boolean isDataFile(String fileName) {
    switch (_format) {
      case AVRO:
        return fileName.endsWith(".avro");
      case GZIPPED_AVRO:
        return fileName.endsWith(".gz");
      case CSV:
        return fileName.endsWith(".csv");
      case JSON:
        return fileName.endsWith(".json");
      case THRIFT:
        return fileName.endsWith(".thrift");
      case PARQUET:
        return fileName.endsWith(".parquet");
      case ORC:
        return fileName.endsWith(".orc");
      default:
        throw new IllegalStateException("Unsupported file format for segment creation: " + _format);
    }
  }
}
