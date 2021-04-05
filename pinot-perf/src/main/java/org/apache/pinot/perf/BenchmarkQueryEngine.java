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

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.requesthandler.OptimizationFlags;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.tools.perf.PerfBenchmarkDriver;
import org.apache.pinot.tools.perf.PerfBenchmarkDriverConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkQueryEngine {
  /** List of query patterns used in the benchmark */
  private static final String[] QUERY_PATTERNS = new String[]{"SELECT count(*) from myTable"};

  /** List of optimization flags to test,
   * see {@link OptimizationFlags#getOptimizationFlags(BrokerRequest)} for the syntax
   * used here. */
  @Param({"", "-multipleOrEqualitiesToInClause"})
  public String optimizationFlags;

  /** List of query patterns indices to run */
  @Param({"0"})
  public int queryPattern;

  /** The table name which contains the offline data, for example "myTable_OFFLINE." */
  private static final String TABLE_NAME = "myTable_OFFLINE";

  /** The directory that contains the unpacked data, for example "/data"
   *
   * In that directory, there should be a "myTable_OFFLINE" directory which contains unpacked segments, so the
   * directory for "/data" should look like "/data/myTable_OFFLINE/mySegment_0/metadata.properties"
   */
  private static final String DATA_DIRECTORY = "/home/someuser/data";

  /**
   * Whether or not to enable profiling information
   */
  private static final boolean ENABLE_PROFILING = false;

  PerfBenchmarkDriver _perfBenchmarkDriver;
  boolean ranOnce = false;

  @Setup
  public void startPinot()
      throws Exception {
    System.out.println("Using table name " + TABLE_NAME);
    System.out.println("Using data directory " + DATA_DIRECTORY);
    System.out.println("Starting pinot");

    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setStartBroker(true);
    conf.setStartController(true);
    conf.setStartServer(true);
    conf.setStartZookeeper(true);
    conf.setRunQueries(false);
    conf.setServerInstanceSegmentTarDir(null);
    conf.setServerInstanceDataDir(DATA_DIRECTORY);
    conf.setConfigureResources(false);
    _perfBenchmarkDriver = new PerfBenchmarkDriver(conf);
    _perfBenchmarkDriver.run();

    File[] segments = new File(DATA_DIRECTORY, TABLE_NAME).listFiles();
    for (File segmentDir : segments) {
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
      _perfBenchmarkDriver.configureTable(TABLE_NAME);
      System.out.println("Adding segment " + segmentDir.getAbsolutePath());
      _perfBenchmarkDriver.addSegment(TABLE_NAME, segmentMetadata);
    }

    ZkClient client = new ZkClient("localhost:2191", 10000, 10000, new ZNRecordSerializer());

    ZNRecord record = client.readData("/PinotPerfTestCluster/EXTERNALVIEW/" + TABLE_NAME);
    while (true) {
      System.out.println("record = " + record);
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

      int onlineSegmentCount = 0;
      for (Map<String, String> instancesAndStates : record.getMapFields().values()) {
        for (String state : instancesAndStates.values()) {
          if (state.equals("ONLINE")) {
            onlineSegmentCount++;
            break;
          }
        }
      }

      System.out.println(onlineSegmentCount + " segments online out of " + segments.length);

      if (onlineSegmentCount == segments.length) {
        break;
      }

      record = client.readData("/PinotPerfTestCluster/EXTERNALVIEW/" + TABLE_NAME);
    }

    ranOnce = false;

    System.out.println(_perfBenchmarkDriver.postQuery(QUERY_PATTERNS[queryPattern], optimizationFlags).toString());
  }

  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int sendQueryToPinot()
      throws Exception {
    return _perfBenchmarkDriver.postQuery(QUERY_PATTERNS[queryPattern], optimizationFlags).get("totalDocs").asInt();
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkQueryEngine.class.getSimpleName()).warmupTime(TimeValue.seconds(30))
            .warmupIterations(4).measurementTime(TimeValue.seconds(30)).measurementIterations(20);

    if (ENABLE_PROFILING) {
      opt = opt.addProfiler(StackProfiler.class,
          "excludePackages=true;excludePackageNames=sun.,java.net.,io.netty.,org.apache.zookeeper.,org.eclipse.jetty.;lines=5;period=1;top=20");
    }

    new Runner(opt.build()).run();
  }
}
