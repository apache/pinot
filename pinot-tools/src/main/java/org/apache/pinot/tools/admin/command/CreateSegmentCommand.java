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
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement CreateSegment command.
 *
 * TODO: Support star-tree creation
 */
public class CreateSegmentCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);

  @Option(name = "-generatorConfigFile", metaVar = "<string>", usage = "Config file for segment generator.")
  private String _generatorConfigFile;

  @Option(name = "-dataDir", metaVar = "<string>", usage = "Directory containing the data.")
  private String _dataDir;

  @Option(name = "-format", metaVar = "<AVRO/CSV/JSON/THRIFT/PARQUET/ORC>", usage = "Input data format.")
  private FileFormat _format;

  @Option(name = "-outDir", metaVar = "<string>", usage = "Name of output directory.")
  private String _outDir;

  @Option(name = "-overwrite", usage = "Overwrite existing output directory.")
  private boolean _overwrite = false;

  @Option(name = "-tableName", metaVar = "<string>", usage = "Name of the table.")
  private String _tableName;

  @Option(name = "-segmentName", metaVar = "<string>", usage = "Name of the segment.")
  private String _segmentName;

  @Option(name = "-timeColumnName", metaVar = "<string>", usage = "Primary time column.")
  private String _timeColumnName;

  @Option(name = "-schemaFile", metaVar = "<string>", usage = "File containing schema for data.")
  private String _schemaFile;

  @Option(name = "-readerConfigFile", metaVar = "<string>", usage = "Config file for record reader.")
  private String _readerConfigFile;

  @Option(name = "-numThreads", metaVar = "<int>", usage = "Parallelism while generating segments, default is 1.")
  private int _numThreads = 1;

  @Option(name = "-postCreationVerification", usage = "Verify segment data file after segment creation. Please ensure you have enough local disk to hold data for verification")
  private boolean _postCreationVerification = false;

  @Option(name = "-retry", metaVar = "<int>", usage = "Number of retries if encountered any segment creation failure, default is 0.")
  private int _retry = 0;

  @SuppressWarnings("FieldCanBeLocal")
  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public CreateSegmentCommand setGeneratorConfigFile(String generatorConfigFile) {
    _generatorConfigFile = generatorConfigFile;
    return this;
  }

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

  public CreateSegmentCommand setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public CreateSegmentCommand setSegmentName(String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  public CreateSegmentCommand setTimeColumnName(String timeColumnName) {
    _timeColumnName = timeColumnName;
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
    return ("CreateSegment  -generatorConfigFile " + _generatorConfigFile + " -dataDir " + _dataDir + " -format "
        + _format + " -outDir " + _outDir + " -overwrite " + _overwrite + " -tableName " + _tableName + " -segmentName "
        + _segmentName + " -timeColumnName " + _timeColumnName + " -schemaFile " + _schemaFile + " -readerConfigFile "
        + _readerConfigFile + " -numThreads " + _numThreads);
  }

  @Override
  public final String getName() {
    return "CreateSegment";
  }

  @Override
  public String description() {
    return "Create pinot segments from provided avro/csv/json input data.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: {}", toString());

    // Load generator config if exist.
    final SegmentGeneratorConfig segmentGeneratorConfig;
    if (_generatorConfigFile != null) {
      segmentGeneratorConfig = JsonUtils.fileToObject(new File(_generatorConfigFile), SegmentGeneratorConfig.class);
    } else {
      segmentGeneratorConfig = new SegmentGeneratorConfig();
    }

    // Load config from segment generator config.
    String configDataDir = segmentGeneratorConfig.getDataDir();
    if (_dataDir == null) {
      if (configDataDir == null) {
        throw new RuntimeException("Must specify dataDir.");
      }
      _dataDir = configDataDir;
    } else {
      if (configDataDir != null && !configDataDir.equals(_dataDir)) {
        LOGGER.warn("Find dataDir conflict in command line and config file, use config in command line: {}", _dataDir);
      }
    }

    FileFormat configFormat = segmentGeneratorConfig.getFormat();
    if (_format == null) {
      if (configFormat == null) {
        throw new RuntimeException("Format cannot be null in config file.");
      }
      _format = configFormat;
    } else {
      if (configFormat != _format && configFormat != FileFormat.AVRO) {
        LOGGER.warn("Find format conflict in command line and config file, use config in command line: {}", _format);
      }
    }

    String configOutDir = segmentGeneratorConfig.getOutDir();
    if (_outDir == null) {
      if (configOutDir == null) {
        throw new RuntimeException("Must specify outDir.");
      }
      _outDir = configOutDir;
    } else {
      if (configOutDir != null && !configOutDir.equals(_outDir)) {
        LOGGER.warn("Find outDir conflict in command line and config file, use config in command line: {}", _outDir);
      }
    }

    if (segmentGeneratorConfig.isOverwrite()) {
      _overwrite = true;
    }

    String configTableName = segmentGeneratorConfig.getTableName();
    if (_tableName == null) {
      if (configTableName == null) {
        throw new RuntimeException("Must specify tableName.");
      }
      _tableName = configTableName;
    } else {
      if (configTableName != null && !configTableName.equals(_tableName)) {
        LOGGER.warn("Find tableName conflict in command line and config file, use config in command line: {}",
            _tableName);
      }
    }

    String configSegmentName = segmentGeneratorConfig.getSegmentName();
    if (_segmentName == null) {
      if (configSegmentName == null) {
        throw new RuntimeException("Must specify segmentName.");
      }
      _segmentName = configSegmentName;
    } else {
      if (configSegmentName != null && !configSegmentName.equals(_segmentName)) {
        LOGGER.warn("Find segmentName conflict in command line and config file, use config in command line: {}",
            _segmentName);
      }
    }

    // Filter out all input files.
    URI dataDirURI = URI.create(_dataDir);
    if (dataDirURI.getScheme() == null) {
      dataDirURI = new File(_dataDir).toURI();
    }
    PinotFS pinotFS = PinotFSFactory.create(dataDirURI.getScheme());

    if (!pinotFS.exists(dataDirURI) || !pinotFS.isDirectory(dataDirURI)) {
      throw new RuntimeException("Data directory " + _dataDir + " not found.");
    }

    // Gather all data files
    String[] dataFilePaths = pinotFS.listFiles(dataDirURI, true);

    if ((dataFilePaths == null) || (dataFilePaths.length == 0)) {
      throw new RuntimeException(
          "Data directory " + _dataDir + " does not contain " + _format.toString().toUpperCase() + " files.");
    }

    LOGGER.info("Accepted files: {}", Arrays.toString(dataFilePaths));

    // Make sure output directory does not already exist, or can be overwritten.
    File outDir = new File(_outDir);
    if (outDir.exists()) {
      if (!_overwrite) {
        throw new IOException("Output directory " + _outDir + " already exists.");
      } else {
        FileUtils.deleteDirectory(outDir);
      }
    }

    // Set other generator configs from command line.
    segmentGeneratorConfig.setDataDir(_dataDir);
    segmentGeneratorConfig.setFormat(_format);
    segmentGeneratorConfig.setOutDir(_outDir);
    segmentGeneratorConfig.setOverwrite(_overwrite);
    segmentGeneratorConfig.setTableName(_tableName);
    segmentGeneratorConfig.setSegmentName(_segmentName);
    if (_timeColumnName != null) {
      segmentGeneratorConfig.setTimeColumnName(_timeColumnName);
    }
    if (_schemaFile != null) {
      if (segmentGeneratorConfig.getSchemaFile() != null && !segmentGeneratorConfig.getSchemaFile()
          .equals(_schemaFile)) {
        LOGGER.warn("Find schemaFile conflict in command line and config file, use config in command line: {}",
            _schemaFile);
      }
      segmentGeneratorConfig.setSchemaFile(_schemaFile);
    }
    if (_readerConfigFile != null) {
      if (segmentGeneratorConfig.getReaderConfigFile() != null && !segmentGeneratorConfig.getReaderConfigFile()
          .equals(_readerConfigFile)) {
        LOGGER.warn("Find readerConfigFile conflict in command line and config file, use config in command line: {}",
            _readerConfigFile);
      }
      segmentGeneratorConfig.setReaderConfigFile(_readerConfigFile);
    }

    ExecutorService executor = Executors.newFixedThreadPool(_numThreads);
    int cnt = 0;
    for (final String dataFilePath : dataFilePaths) {
      final int segCnt = cnt;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          for (int curr = 0; curr <= _retry; curr++) {
            File localDir = new File(UUID.randomUUID().toString());
            try {
              SegmentGeneratorConfig config = new SegmentGeneratorConfig(segmentGeneratorConfig);
              URI dataFileUri = URI.create(dataFilePath);
              String[] splits = dataFilePath.split("/");
              String fileName = splits[splits.length - 1];
              if (!isDataFile(fileName)) {
                return;
              }
              File localFile = new File(localDir, fileName);
              pinotFS.copyToLocalFile(dataFileUri, localFile);
              config.setInputFilePath(localFile.getAbsolutePath());
              config.setSegmentName(_segmentName + "_" + segCnt);
              Schema schema = Schema.fromFile(new File(_schemaFile));
              config.setSchema(schema);

              final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
              switch (config.getFormat()) {
                case PARQUET:
                  config.setRecordReaderPath("org.apache.pinot.plugin.inputformat.ParquetRecordReader");
                  driver.init(config);
                  break;
                case ORC:
                  config.setRecordReaderPath("org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
                  driver.init(config);
                  break;
                case CSV:
                  RecordReader csvRecordReader = new CSVRecordReader();
                  CSVRecordReaderConfig readerConfig = null;
                  if (_readerConfigFile != null) {
                    readerConfig = JsonUtils.fileToObject(new File(_readerConfigFile), CSVRecordReaderConfig.class);
                  }
                  csvRecordReader.init(localFile, schema, readerConfig, SchemaFieldExtractorUtils.extract(schema));
                  driver.init(config, csvRecordReader);
                  break;
                default:
                  driver.init(config);
              }
              driver.build();
              if (_postCreationVerification) {
                if (!verifySegment(new File(config.getOutDir(), driver.getSegmentName()))) {
                  throw new RuntimeException("Pinot segment is corrupted, please try to recreate it.");
                } else {
                  LOGGER.info("Post segment creation verification is succeed for segment {}.", driver.getSegmentName());
                }
              }
              break;
            } catch (Exception e) {
              LOGGER.error("Got exception during segment creation.", e);
              if (curr == _retry) {
                throw new RuntimeException(e);
              } else {
                LOGGER.error("Failed to create Pinot segment, retry: {}/{}", curr + 1, _retry);
              }
            } finally {
              FileUtils.deleteQuietly(localDir);
            }
          }
        }
      });
      cnt += 1;
    }

    executor.shutdown();
    return executor.awaitTermination(1, TimeUnit.HOURS);
  }

  private boolean verifySegment(File indexDir) {
    File localTempDir =
        new File(FileUtils.getTempDirectory(), org.apache.pinot.common.utils.FileUtils.getRandomFileName());
    try {
      try {
        localTempDir.getParentFile().mkdirs();
        URI indexDirUri = URI.create(indexDir.toString());
        PinotFS pinotFs = PinotFSFactory.create(indexDirUri.getScheme());
        pinotFs.copyToLocalFile(indexDirUri, localTempDir);
      } catch (Exception e) {
        LOGGER.error("Failed to copy segment {} to local directory {} for verification.", indexDir, localTempDir, e);
        return false;
      }
      try {
        ImmutableSegment segment = ImmutableSegmentLoader.load(localTempDir, ReadMode.mmap);
        LOGGER.info("Successfully loaded Pinot segment {} (size: {} Bytes) from {}.", segment.getSegmentName(),
            segment.getSegmentSizeBytes(), localTempDir);
        segment.destroy();
      } catch (Exception e) {
        LOGGER.error("Failed to load segment from {}.", localTempDir, e);
        return false;
      }
      return true;
    } finally {
      FileUtils.deleteQuietly(localTempDir);
    }
  }

  protected boolean isDataFile(String fileName) {
    switch (_format) {
      case AVRO:
      case GZIPPED_AVRO:
        return fileName.endsWith(".avro");
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
