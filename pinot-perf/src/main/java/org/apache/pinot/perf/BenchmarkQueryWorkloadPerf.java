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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.perf.PerfBenchmarkDriver;
import org.apache.pinot.tools.perf.PerfBenchmarkDriverConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
//@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkQueryWorkloadPerf {

    /**
     * Whether or not to enable profiling information
     */
    private static final boolean ENABLE_PROFILING = true;

    PerfBenchmarkDriver _perfBenchmarkDriver;
    boolean _ranOnce = false;

    private PinotHelixResourceManager _pinotHelixResourceManager;

    private int countFetched = 0;

    @Setup
    public void startPinot()
            throws Exception {
//    System.out.println("Using table name " + TABLE_NAME);
//    System.out.println("Using data directory " + DATA_DIRECTORY);
        System.out.println("Starting pinot");

        PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
        conf.setStartBroker(true);
        conf.setStartController(true);
        conf.setStartServer(true);
        conf.setStartZookeeper(true);
        conf.setRunQueries(false);
        conf.setServerInstanceSegmentTarDir(null);
        conf.setServerInstanceDataDir(null);
        conf.setConfigureResources(false);
        _perfBenchmarkDriver = new PerfBenchmarkDriver(conf);
        _perfBenchmarkDriver.run();

        _ranOnce = false;
        _pinotHelixResourceManager = _perfBenchmarkDriver.getHelixResourceManager();
//    createWorkload();
//    System.out.println("Created workload");
        System.out.println("**** Creating InstanceConfigs ****");
        createInstanceConfigs();
        System.out.println("**** Created InstanceConfigs ****");
        System.out.println("**** Creating tables ****");
        createTables();
        System.out.println("**** Created tables ****");
    }

    private void createWorkload() {
        EnforcementProfile enforcementProfile = new EnforcementProfile(100, 100, 100L);
        for (int i = 0; i < 10000; i++) {
            String queryWorkloadName = "queryWorkload" + i;
            List<String> values = new ArrayList<>();
            values.add("myTag" + i + "_OFFLINE");
            PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, values);
            NodeConfig nodeConfig = new NodeConfig(enforcementProfile, propagationScheme);
            Map<NodeConfig.Type, NodeConfig> nodeConfigMap = Map.of(NodeConfig.Type.LEAF_NODE, nodeConfig);
            QueryWorkloadConfig queryWorkloadConfig = new QueryWorkloadConfig(queryWorkloadName, nodeConfigMap);
            _pinotHelixResourceManager.setQueryWorkloadConfig(queryWorkloadConfig);
        }
    }

    private void createTables() {
        // Create a thread pool; adjust pool size as needed
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
        for (int i = 0; i < 10000; i++) {
            final int index = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    String serverTag = "myTag" + index;
                    String tableName = "myTable" + index;
                    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
                            .setTableName(tableName)
                            .setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy")
                            .setNumReplicas(1)
                            .setBrokerTenant("DefaultTenant")
                            .setServerTenant(serverTag)
                            .setLoadMode("HEAP")
                            .setSegmentVersion(null)
                            .build();
                    ZKMetadataProvider.createTableConfig(helixManager.getHelixPropertyStore(), tableConfig);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            futures.add(future);
        }

        // Set up a scheduled task to periodically check progress
        ScheduledExecutorService progressScheduler = Executors.newSingleThreadScheduledExecutor();
        progressScheduler.scheduleAtFixedRate(() -> {
            long doneCount = futures.stream().filter(CompletableFuture::isDone).count();
            System.out.println("Progress: " + doneCount + " / " + futures.size() + " completed.");
        }, 1, 10, TimeUnit.SECONDS);

        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Shutdown both executors
        executor.shutdown();
        progressScheduler.shutdownNow();
    }

    private void createInstanceConfigs() {
        HelixDataAccessor helixDataAccessor = _pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            final int index = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                String instanceName = "myInstance" + index;
                InstanceConfig instanceConfig = new InstanceConfig(instanceName);
                String tag = "myTag" + index + "_OFFLINE";
                instanceConfig.addTag(tag);
                helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().instanceConfig(instanceName), instanceConfig);
            }, executor);
            futures.add(future);
        }

        // Set up a scheduled task to periodically check progress
        ScheduledExecutorService progressScheduler = Executors.newSingleThreadScheduledExecutor();
        progressScheduler.scheduleAtFixedRate(() -> {
            long doneCount = futures.stream().filter(CompletableFuture::isDone).count();
            System.out.println("InstanceConfigs Progress: " + doneCount + " / " + futures.size() + " completed.");
        }, 1, 10, TimeUnit.SECONDS);

        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        executor.shutdown();
        progressScheduler.shutdownNow();
    }

//  @Benchmark
//  @BenchmarkMode(Mode.SampleTime)
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  public void getWorkloadConfig() {
//    EnforcementProfile enforcementProfile = new EnforcementProfile(100, 100, 100L);
//    String queryWorkloadName = "queryWorkload";
//    List<String> values = new ArrayList<>();
//    for (int i = 0; i < 10000; i++) {
//      values.add("myTable" + i + "_OFFLINE");
//    }
//    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, values);
//    NodeConfig nodeConfig = new NodeConfig(enforcementProfile, propagationScheme);
//    Map<NodeConfig.Type, NodeConfig> nodeConfigMap = Map.of(NodeConfig.Type.LEAF_NODE, nodeConfig);
//    QueryWorkloadConfig queryWorkloadConfig = new QueryWorkloadConfig(queryWorkloadName, nodeConfigMap);
//    _pinotHelixResourceManager.setQueryWorkloadConfig(queryWorkloadConfig);
//  }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void getWorkloadConfig() {
        EnforcementProfile enforcementProfile = new EnforcementProfile(100, 100, 100L);
        String queryWorkloadName = "queryWorkload";
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            values.add("myTable" + i + "_OFFLINE");
        }
        PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, values);
        NodeConfig nodeConfig = new NodeConfig(enforcementProfile, propagationScheme);
        Map<NodeConfig.Type, NodeConfig> nodeConfigMap = Map.of(NodeConfig.Type.LEAF_NODE, nodeConfig);
        QueryWorkloadConfig queryWorkloadConfig = new QueryWorkloadConfig(queryWorkloadName, nodeConfigMap);
        _pinotHelixResourceManager.setQueryWorkloadConfig(queryWorkloadConfig);
    }

//  public static void main(String[] args) throws Exception {
//    for (int threads : List.of(1)) {
//      System.out.println("Running with " + threads + " threads");
//      new Runner(new OptionsBuilder()
//          .include(BenchmarkQueryWorkloadPerf.class.getSimpleName())
//          .forks(1)
//          .warmupIterations(1)
//          .measurementIterations(2)
//          .measurementTime(TimeValue.seconds(60))
//          .threads(threads)
//          .build()
//      ).run();
//    }
//  }

    public static void main(String[] args)
            throws Exception {
        ChainedOptionsBuilder opt =
                new OptionsBuilder().include(BenchmarkQueryWorkloadPerf.class.getSimpleName()).warmupTime(TimeValue.seconds(10))
                        .warmupIterations(1).measurementTime(TimeValue.seconds(120)).measurementIterations(1);

        if (ENABLE_PROFILING) {
            opt = opt.addProfiler(StackProfiler.class,
                    "excludePackages=true;excludePackageNames=sun.,java.net.,io.netty.,org.apache.zookeeper.,org.eclipse.jetty"
                            + ".;lines=5;period=1;top=20");
        }

        new Runner(opt.build()).run();
    }
}
