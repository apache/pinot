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
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.parquet.data.readers.ParquetRecordReader;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement CreateSegment command.
 *
 * CreateSegmentCommandV2 reuses configurations from existing table configs and table schema, which will
 * be simpler for users to try out.
 *
 */
public class CreateSegmentCommandV2 extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommandV2.class);

  @Option(name = "-schemaFile", metaVar = "<string>", usage = "File containing schema for data.", required = true)
  private String _schemaFile;

  @Option(name = "-tableConfig", metaVar = "<string>", usage = "File containing schema for data.", required = true)
  private String _tableConfigFile;

  @Option(name = "-dataDir", metaVar = "<string>", usage = "Directory containing the input data.", required = true)
  private String _dataDir;

  @Option(name = "-inputFormat", metaVar = "<string>", usage = "Input File format (CSV/JSON/AVRO/Thrift/Parquet).")
  private FileFormat _inputFormat;

  @Option(name = "-outDir", metaVar = "<string>", usage = "Name of output directory.", required = true)
  private String _outDir;

  @Option(name = "-overwrite", usage = "Overwrite existing output directory.")
  private boolean _overwrite = false;

  @Option(name = "-numThreads", metaVar = "<int>", usage = "Parallelism while generating segments, default is 1.")
  private int _numThreads = 1;

  @SuppressWarnings("FieldCanBeLocal")
  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public CreateSegmentCommandV2 setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  public CreateSegmentCommandV2 setOutDir(String outDir) {
    _outDir = outDir;
    return this;
  }

  public CreateSegmentCommandV2 setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
    return this;
  }

  public CreateSegmentCommandV2 setNumThreads(int numThreads) {
    _numThreads = numThreads;
    return this;
  }

  @Override
  public final String getName() {
    return "CreateNewSegment";
  }

  @Override
  public String description() {
    return "Create pinot segments from provided avro/csv/json/thrift/parquet input data.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return ("CreateSegment " + " -dataDir " + _dataDir + " -inputFormat " + _inputFormat + " -outDir " + _outDir
        + " -overwrite " + _overwrite + " -schemaFile " + _schemaFile + " -tableConfigFile " + _tableConfigFile
        + " -numThreads " + _numThreads);
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: {}", toString());

    Schema schema = Schema.fromFile(new File(_schemaFile));

    TableConfig tableConfig = TableConfig.fromJsonConfig(JsonUtils.fileToJsonNode(new File(_tableConfigFile)));

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
          "Data directory " + _dataDir + " does not contain " + _inputFormat.toString().toUpperCase() + " files.");
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

    ExecutorService executor = Executors.newFixedThreadPool(_numThreads);
    int cnt = 0;

    for (final Path dataFilePath : dataFilePaths) {
      final int segCnt = cnt;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
            config.setDataDir(_dataDir);
            config.setOutDir(_outDir);
            config.setOverwrite(_overwrite);
            String localFile = dataFilePath.getName();
            if (_inputFormat == null) {
              _inputFormat = FileFormat.valueOf(FilenameUtils.getExtension(localFile).toUpperCase());
            }
            config.setFormat(_inputFormat);
            Path localFilePath = new Path(localFile);
            dataDirPath.getFileSystem(new Configuration()).copyToLocalFile(dataFilePath, localFilePath);
            String md5;
            try (InputStream is = Files.newInputStream(Paths.get(localFile))) {
              md5 = org.apache.commons.codec.digest.DigestUtils.shaHex(is);
            }
            config.setInputFilePath(localFile);
            config.setSegmentName(config.getTableName() + "_" + md5 + "_" + segCnt);

            final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
            switch (config.getFormat()) {
              case PARQUET:
                RecordReader parquetRecordReader = new ParquetRecordReader();
                parquetRecordReader.init(config);
                driver.init(config, parquetRecordReader);
                break;
              default:
                driver.init(config);
            }
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
    return fileName.endsWith(".avro") || fileName.endsWith(".csv") || fileName.endsWith(".json") || fileName
        .endsWith(".thrift") || fileName.endsWith(".parquet");
  }
}
