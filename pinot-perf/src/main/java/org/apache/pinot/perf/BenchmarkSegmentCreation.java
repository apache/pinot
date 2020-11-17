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
package org.apache.pinot.perf;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.Runner;

@State(Scope.Benchmark)
public class BenchmarkSegmentCreation {
  private static final String TABLE_CONFIG_PATH = "/tmp/segment-creation/config.json";
  private static final String TABLE_SCHEMA_PATH = "/tmp/segment-creation/schema.json";
  private static final String OUT_DIR_PATH = "/tmp/segment-creation/segments/";
  private static final String DATA_DIR_PATH = "/tmp/segment-creation/csv/";

  private TableConfig tableConfig;
  private Schema tableSchema;
  private String rawTableName;
  private File outDir;
  private File dataDir;
  private List<String> dataFiles;

  private List<String> getDataFiles(File dataDir) {
    List<String> dataFiles = new ArrayList<>();
    getDataFilesHelper(dataDir.listFiles(), dataFiles);
    return dataFiles;
  }

  private void getDataFilesHelper(File[] files, List<String> dataFiles) {
    for (File file : files) {
      if (file.isDirectory()) {
        getDataFilesHelper(file.listFiles(), dataFiles);
      } else {
        if (file.getName().endsWith(".csv")) {
          dataFiles.add(file.getPath());
        }
      }
    }
  }

  @Setup
  public void setUp() {
    Preconditions.checkArgument(TABLE_CONFIG_PATH != null, "'TABLE_CONFIG_PATH' must be specified");
    Preconditions.checkArgument(TABLE_SCHEMA_PATH != null, "'TABLE_SCHEMA_PATH' must be specified");
    Preconditions.checkArgument(DATA_DIR_PATH != null, "'DATA_DIR_PATH' must be specified");
    Preconditions.checkArgument(OUT_DIR_PATH != null, "'OUT_DIR_PATH' must be specified");

    try {
      tableConfig = JsonUtils.fileToObject(new File(TABLE_CONFIG_PATH), TableConfig.class);
      rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while reading table config from file: " + TABLE_CONFIG_PATH, e);
    }

    try {
      tableSchema = JsonUtils.fileToObject(new File(TABLE_SCHEMA_PATH), Schema.class);
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while reading schema from file: " + TABLE_SCHEMA_PATH, e);
    }

    dataDir = new File(DATA_DIR_PATH);
    Preconditions.checkArgument(dataDir.isDirectory(), "'dataDir': '%s' is not a directory", DATA_DIR_PATH);

    dataFiles = getDataFiles(dataDir);
    Preconditions.checkState(!dataFiles.isEmpty(), "Failed to find any CSV data file under directory: '%s'", DATA_DIR_PATH);

    outDir = new File(OUT_DIR_PATH);

    try {
      if (outDir.exists()) {
        FileUtils.forceDelete(outDir);
      }
      FileUtils.forceMkdir(outDir);
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while creating output directory: " + OUT_DIR_PATH, e);
    }

    Preconditions.checkState(outDir.exists(), "Failed to create output directory: '%s'", OUT_DIR_PATH);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void segmentCreationFromCSV()
      throws Exception {
    for (int i = 0; i < dataFiles.size(); i++) {
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, tableSchema);

        segmentGeneratorConfig.setInputFilePath(dataFiles.get(i));
        segmentGeneratorConfig.setFormat(FileFormat.CSV);
        segmentGeneratorConfig.setOutDir(outDir.getPath());
        segmentGeneratorConfig.setTableName(rawTableName);
        segmentGeneratorConfig.setSequenceId(i);

        SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();

        driver.init(segmentGeneratorConfig);
        driver.build();
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder()
          .include(BenchmarkSegmentCreation.class.getSimpleName())
          .warmupIterations(5)
          .measurementIterations(10)
          .forks(5);

    new Runner(opt.build()).run();
  }
}
