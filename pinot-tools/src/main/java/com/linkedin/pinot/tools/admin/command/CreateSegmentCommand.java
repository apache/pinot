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
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement CreateSegment command.
 *
 */
public class CreateSegmentCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);

  @Option(name = "-generatorConfigFile", required = false, metaVar = "<string>",
      usage = "Config file for segment generator.")
  private String _generatorConfigFile;

  @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the data.")
  private String _dataDir;

  @Option(name = "-format", required = false, metaVar = "<AVRO/CSV/JSON>", usage = "Input data format.")
  private FileFormat _format;

  @Option(name = "-outDir", required = false, metaVar = "<string>", usage = "Name of output directory.")
  private String _outDir;

  @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
  private boolean _overwrite = false;

  @Option(name = "-tableName", required = false, metaVar = "<string>", usage = "Name of the table.")
  private String _tableName;

  @Option(name = "-segmentName", required = false, metaVar = "<string>", usage = "Name of the segment.")
  private String _segmentName;

  @Option(name = "-schemaFile", required = false, metaVar = "<string>", usage = "File containing schema for data.")
  private String _schemaFile;

  @Option(name = "-readerConfigFile", required = false, metaVar = "<string>", usage = "Config file for record reader.")
  private String _readerConfigFile;

  @Option(name = "-enableStarTreeIndex", required = false, usage = "Enable Star Tree Index.")
  boolean _enableStarTreeIndex = false;

  @Option(name = "-starTreeIndexSpecFile", required = false, metaVar = "<string>",
      usage = "Config file for star tree index.")
  private String _starTreeIndexSpecFile;

  @Option(name = "-numThreads", required = false, metaVar = "<int>",
      usage = "Parallelism while generating segments, default is 1.")
  private int _numThreads = 1;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
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

  public CreateSegmentCommand setNumThreads(int numThreads) {
    _numThreads = numThreads;
    return this;
  }

  @Override
  public String toString() {
    return ("CreateSegment  -generatorConfigFile " + _generatorConfigFile + " -dataDir " + _dataDir + " -format "
        + _format + " -outDir " + _outDir + " -overwrite " + _overwrite + " -tableName " + _tableName + " -segmentName "
        + _segmentName + " -schemaFile " + _schemaFile + " -readerConfigFile " + _readerConfigFile
        + " -enableStarTreeIndex " + _enableStarTreeIndex + " -starTreeIndexSpecFile " + _starTreeIndexSpecFile
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
      segmentGeneratorConfig =
          new ObjectMapper().readValue(new File(_generatorConfigFile), SegmentGeneratorConfig.class);
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

    // Filter out all input files.
    File dir = new File(_dataDir);
    if (!dir.exists() || !dir.isDirectory()) {
      throw new RuntimeException("Data directory " + _dataDir + " not found.");
    }

    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(_format.toString().toLowerCase());
      }
    });

    if ((files == null) || (files.length == 0)) {
      throw new RuntimeException(
          "Data directory " + _dataDir + " does not contain " + _format.toString().toUpperCase() + " files.");
    }

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
    if (_schemaFile != null) {
      if (segmentGeneratorConfig.getSchemaFile() != null && !segmentGeneratorConfig.getSchemaFile()
          .equals(_schemaFile)) {
        LOGGER.warn("Find schemaFile conflict in command line and config file, use config in command line: {}",
            _schemaFile);
      }
      segmentGeneratorConfig.setSchemaFile(_schemaFile);
    }

    String configSegmentName = segmentGeneratorConfig.getSegmentName();
    if (_segmentName == null) {
      if (configSegmentName == null) {
        if (_schemaFile != null) {
          LOGGER.warn("Segment name not specified. Deriving segment name from schema name");
          // Derive segment name from schema name
          File file = new File(_schemaFile);
          _segmentName = file.getName().replaceAll("\\..*", "");
        } else {
          throw new RuntimeException("Segment Name not specified");
        }
      } else {
        _segmentName = configSegmentName;
      }
    } else {
      if (configSegmentName != null && !configSegmentName.equals(_segmentName)) {
        LOGGER.warn("Find segmentName conflict in command line and config file, use config in command line: {}",
            _segmentName);
      }
    }
    segmentGeneratorConfig.setSegmentName(_segmentName);

    if (_readerConfigFile != null) {
      if (segmentGeneratorConfig.getReaderConfigFile() != null && !segmentGeneratorConfig.getReaderConfigFile()
          .equals(_readerConfigFile)) {
        LOGGER.warn("Find readerConfigFile conflict in command line and config file, use config in command line: {}",
            _readerConfigFile);
      }
      segmentGeneratorConfig.setReaderConfigFile(_readerConfigFile);
    }
    if (_enableStarTreeIndex) {
      segmentGeneratorConfig.setEnableStarTreeIndex(true);
    }
    if (_starTreeIndexSpecFile != null) {
      if (segmentGeneratorConfig.getStarTreeIndexSpecFile() != null
          && !segmentGeneratorConfig.getStarTreeIndexSpecFile().equals(_starTreeIndexSpecFile)) {
        LOGGER.warn(
            "Find starTreeIndexSpecFile conflict in command line and config file, use config in command line: {}",
            _starTreeIndexSpecFile);
      }
      segmentGeneratorConfig.setStarTreeIndexSpecFile(_starTreeIndexSpecFile);
    }

    ExecutorService executor = Executors.newFixedThreadPool(_numThreads);
    int cnt = 0;
    for (final File file : files) {
      final int segCnt = cnt;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            SegmentGeneratorConfig config = new SegmentGeneratorConfig(segmentGeneratorConfig);
            config.setInputFilePath(file.getAbsolutePath());
            config.setSegmentName(_segmentName + "_" + segCnt);
            config.loadConfigFiles();

            final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
            driver.init(config);
            driver.build();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
      cnt += 1;
    }

    executor.shutdown();
    return executor.awaitTermination(1, TimeUnit.HOURS);
  }
}
