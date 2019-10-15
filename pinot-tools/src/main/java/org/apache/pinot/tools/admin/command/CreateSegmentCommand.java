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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.orc.data.readers.ORCRecordReader;
import org.apache.pinot.parquet.data.readers.ParquetRecordReader;
import org.apache.pinot.startree.hll.HllConfig;
import org.apache.pinot.startree.hll.HllConstants;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement CreateSegment command.
 *
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

  @Option(name = "-enableStarTreeIndex", usage = "Enable Star Tree Index.")
  boolean _enableStarTreeIndex = false;

  @Option(name = "-starTreeIndexSpecFile", metaVar = "<string>", usage = "Config file for star tree index.")
  private String _starTreeIndexSpecFile;

  @Option(name = "-hllSize", metaVar = "<5,6,7,8,9>", usage = "HLL size (log scale), default is 9.")
  private int _hllSize = 9;

  @Option(name = "-hllColumns", metaVar = "<string>", usage = "Columns to compute HLL.")
  private String _hllColumns;

  @Option(name = "-hllSuffix", metaVar = "<string>", usage = "Suffix for the derived HLL columns")
  private String _hllSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;

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

  public CreateSegmentCommand setEnableStarTreeIndex(boolean enableStarTreeIndex) {
    _enableStarTreeIndex = enableStarTreeIndex;
    return this;
  }

  public CreateSegmentCommand setStarTreeIndexSpecFile(String starTreeIndexSpecFile) {
    _starTreeIndexSpecFile = starTreeIndexSpecFile;
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

  public void setHllSize(int hllSize) {
    _hllSize = hllSize;
  }

  public void setHllColumns(String hllColumns) {
    _hllColumns = hllColumns;
  }

  public void setHllSuffix(String hllSuffix) {
    _hllSuffix = hllSuffix;
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
        + _readerConfigFile + " -enableStarTreeIndex " + _enableStarTreeIndex + " -starTreeIndexSpecFile "
        + _starTreeIndexSpecFile + " -hllSize " + _hllSize + " -hllColumns " + _hllColumns + " -hllSuffix " + _hllSuffix
        + " -numThreads " + _numThreads);
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
    final Path dataDirPath = new Path(_dataDir);
    FileSystem fileSystem = FileSystem.get(URI.create(_dataDir), new Configuration());

    if (!fileSystem.exists(dataDirPath) || !fileSystem.isDirectory(dataDirPath)) {
      throw new RuntimeException("Data directory " + _dataDir + " not found.");
    }

    // Gather all data files
    List<Path> dataFilePaths = getDataFilePaths(dataDirPath);

    if ((dataFilePaths == null) || (dataFilePaths.size() == 0)) {
      throw new RuntimeException(
          "Data directory " + _dataDir + " does not contain " + _format.toString().toUpperCase() + " files.");
    }

    LOGGER.info("Accepted files: {}", Arrays.toString(dataFilePaths.toArray()));

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

    if (_starTreeIndexSpecFile != null) {
      StarTreeIndexSpec starTreeIndexSpec = StarTreeIndexSpec.fromFile(new File(_starTreeIndexSpecFile));

      // Specifying star-tree index file enables star tree generation, even if _enableStarTreeIndex is not specified.
      segmentGeneratorConfig.enableStarTreeIndex(starTreeIndexSpec);
    } else if (_enableStarTreeIndex) {
      segmentGeneratorConfig.enableStarTreeIndex(null);
    }

    if (_hllColumns != null) {
      String[] hllColumns = StringUtils.split(StringUtils.deleteWhitespace(_hllColumns), ',');
      if (hllColumns.length != 0) {
        LOGGER.info("Derive HLL fields on columns: {} with size: {} and suffix: {}", Arrays.toString(hllColumns),
            _hllSize, _hllSuffix);
        HllConfig hllConfig = new HllConfig(_hllSize);
        hllConfig.setColumnsToDeriveHllFields(new HashSet<>(Arrays.asList(hllColumns)));
        hllConfig.setHllDeriveColumnSuffix(_hllSuffix);
        segmentGeneratorConfig.setHllConfig(hllConfig);
      }
    }

    ExecutorService executor = Executors.newFixedThreadPool(_numThreads);
    int cnt = 0;
    for (final Path dataFilePath : dataFilePaths) {
      final int segCnt = cnt;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          for (int curr = 0; curr <= _retry; curr++) {
            try {
              SegmentGeneratorConfig config = new SegmentGeneratorConfig(segmentGeneratorConfig);

              String localFile = dataFilePath.getName();
              Path localFilePath = new Path(localFile);
              dataDirPath.getFileSystem(new Configuration()).copyToLocalFile(dataFilePath, localFilePath);
              config.setInputFilePath(localFile);
              config.setSegmentName(_segmentName + "_" + segCnt);
              config.loadConfigFiles();

              final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
              switch (config.getFormat()) {
                case PARQUET:
                  RecordReader parquetRecordReader = new ParquetRecordReader();
                  parquetRecordReader.init(config);
                  driver.init(config, parquetRecordReader);
                  break;
                case ORC:
                  RecordReader orcRecordReader = new ORCRecordReader();
                  orcRecordReader.init(config);
                  driver.init(config, orcRecordReader);
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
    File localTempDir = FileUtils.getTempDirectory();
    try {
      try {
        localTempDir.getParentFile().mkdirs();
        FileSystem.get(URI.create(indexDir.toString()), new Configuration())
            .copyToLocalFile(new Path(indexDir.toString()), new Path(localTempDir.toString()));
      } catch (IOException e) {
        LOGGER.error("Failed to copy segment {} to local directory for verification.", indexDir, e);
        return false;
      }
      try {
        ImmutableSegment segment = ImmutableSegmentLoader.load(localTempDir, ReadMode.mmap);
        LOGGER.info("Successfully loaded Pinot segment {} (size: {} Bytes).", segment.getSegmentName(),
            segment.getSegmentSizeBytes());
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

  protected List<Path> getDataFilePaths(Path pathPattern)
      throws IOException {
    List<Path> tarFilePaths = new ArrayList<>();
    FileSystem fileSystem = FileSystem.get(pathPattern.toUri(), new Configuration());
    getDataFilePathsHelper(fileSystem, fileSystem.globStatus(pathPattern), tarFilePaths);
    return tarFilePaths;
  }

  protected void getDataFilePathsHelper(FileSystem fileSystem, FileStatus[] fileStatuses, List<Path> tarFilePaths)
      throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      if (fileStatus.isDirectory()) {
        getDataFilePathsHelper(fileSystem, fileSystem.listStatus(path), tarFilePaths);
      } else {
        if (isDataFile(path.getName())) {
          tarFilePaths.add(path);
        }
      }
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
