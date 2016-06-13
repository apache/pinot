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
package com.linkedin.pinot.perf;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkQueryEngine {
  private static final int EXPECTED_NUM_DOCS_SCANNED = 25000000;
  private static final int EXPECTED_TOTAL_DOCS_COUNT = 25000000;
  private static final String QUERY = "select count(*) from sTest group by daysSinceEpoch";
  private static final String TABLE_NAME = "sTest_OFFLINE";
  private static final String DATA_DIRECTORY = "/Users/jfim/index_dir";

  PerfBenchmarkDriver _perfBenchmarkDriver;

  @Setup
  public void startPinot() throws Exception {
    System.out.println("Using table name " + TABLE_NAME);
    System.out.println("Using data directory " + DATA_DIRECTORY);
    System.out.println("Starting pinot");

    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setStartBroker(true);
    conf.setStartController(true);
    conf.setStartServer(true);
    conf.setStartZookeeper(true);
    conf.setUploadIndexes(false);
    conf.setRunQueries(false);
    conf.setServerInstanceSegmentTarDir(null);
    conf.setServerInstanceDataDir(DATA_DIRECTORY);
    conf.setConfigureResources(false);
    _perfBenchmarkDriver = new PerfBenchmarkDriver(conf);
    _perfBenchmarkDriver.run();

    Set<String> tables = new HashSet<String>();
    File[] segments = new File(DATA_DIRECTORY, TABLE_NAME).listFiles();
    for (File segmentDir : segments) {
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
      if (!tables.contains(segmentMetadata.getTableName())) {
        _perfBenchmarkDriver.configureTable(segmentMetadata.getTableName());
        tables.add(segmentMetadata.getTableName());
      }
      System.out.println("Adding segment " + segmentDir.getAbsolutePath());
      _perfBenchmarkDriver.addSegment(segmentMetadata);
    }

    System.out.println("Waiting for 10s for everything to be loaded");
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int sendQueryToPinot() throws Exception {
    JSONObject returnValue = _perfBenchmarkDriver.postQuery(QUERY);
    if (returnValue.getInt("numDocsScanned") != EXPECTED_NUM_DOCS_SCANNED || returnValue.getInt("totalDocs") != EXPECTED_TOTAL_DOCS_COUNT) {
      System.out.println("returnValue = " + returnValue);
      throw new RuntimeException("Unexpected number of docs scanned/total docs");
    }
    return returnValue.getInt("totalDocs");
  }

  public static void main(String[] args) throws Exception {

    Options opt = new OptionsBuilder()
        .include(BenchmarkQueryEngine.class.getSimpleName())
        .forks(1)
        .warmupTime(TimeValue.seconds(6))
        .warmupIterations(10)
        .measurementTime(TimeValue.seconds(6))
        .measurementIterations(10)
        .addProfiler(StackProfiler.class)
        .build();

    new Runner(opt).run();
  }
}
