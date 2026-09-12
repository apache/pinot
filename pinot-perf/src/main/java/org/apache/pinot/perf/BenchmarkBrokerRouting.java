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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.instanceselector.BalancedInstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.BaseInstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelectorConfig;
import org.apache.pinot.broker.routing.instanceselector.ReplicaGroupInstanceSelector;
import org.apache.pinot.broker.routing.manager.BaseBrokerRoutingManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.broker.routing.segmentselector.DefaultSegmentSelector;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;



/// Measures the actual public broker routing path with immutable shared routing state.
/// Balanced and replica_group modes run production selection, observable pool meters, and grouping.
/// Preselected mode substitutes a plain fixed selector to isolate production grouping (no timed Mockito calls).
/// Metadata initialization and validation are outside timing; there is no network, SQL planning or query execution.
/// Optional segments have recent metadata and OFFLINE external-view replicas; unavailable segments have ERROR replicas.
/// All modes use cyclic RF3 assignments, including replica_group: this measures its algorithm without mirrored
/// placement.
/// Pool meters are shared across JMH workers, so concurrent results include normal meter contention.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class BenchmarkBrokerRouting {
  private static final String TABLE = "routing_bench_OFFLINE";
  private static final int REPLICAS = 3;
  private static final long FIXTURE_TIME_MS = 1_800_000_000_000L;

  @Param({"100", "10000"})
  public int _segments;
  @Param({"100"})
  public int _servers;
  @Param({"1", "4"})
  public int _pools;
  @Param({"balanced", "replica_group", "preselected"})
  public String _selector;
  @Param({"0"})
  public int _optionalPercent;
  @Param({"0"})
  public int _unavailablePercent;

  private BrokerRoutingManager _manager;
  private BrokerRequest _request;
  private Map<String, String> _fixedRequired;
  private Map<String, String> _fixedOptional;
  private Set<String> _segmentNames;
  private Map<String, List<String>> _candidates;
  private Set<String> _optionalSegments;
  private Set<String> _unavailableSegments;
  private final List<PinotMeter> _poolMeters = new ArrayList<>();

  @SuppressWarnings("unchecked")
  @Setup
  public void setup() throws Exception {
    if (_segments < 0 || _servers < REPLICAS || _pools < 1 || _pools > _servers
        || _optionalPercent < 0 || _unavailablePercent < 0 || _optionalPercent + _unavailablePercent > 100) {
      throw new IllegalArgumentException("Invalid dimensions");
    }
    if (!Set.of("balanced", "replica_group", "preselected").contains(_selector)) {
      throw new IllegalArgumentException("Unknown selector: " + _selector);
    }
    Configurator.setRootLevel(Level.OFF);
    PinotMetricUtils.init(new PinotConfiguration(Map.of("metricsRegistryRegistrationListeners", List.of())));
    BrokerMetrics metrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    for (int pool = 0; pool < _pools; pool++) {
      _poolMeters.add(metrics.addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 0L,
          BrokerMetrics.getTagForPreferredPool(Map.of()), Integer.toString(pool)));
    }
    _manager = new BrokerRoutingManager(metrics, null, new PinotConfiguration(Map.of(
        Broker.CONFIG_OF_ROUTING_ASSIGNMENT_CHANGE_PROCESS_PARALLELISM, 1)));
    Map<String, ServerInstance> enabled = _manager.getEnabledServerInstanceMap();
    List<String> instanceIds = new ArrayList<>();
    for (int server = 0; server < _servers; server++) {
      String host = String.format(Locale.ROOT, "routing-%05d.prod.example.com", server);
      String id = ("Server_" + host + "_8098").intern();
      InstanceConfig config = new InstanceConfig(id);
      config.setHostName(host);
      config.setPort("8098");
      config.getRecord().setMapField(InstanceUtils.POOL_KEY,
          Map.of("routing_bench", Integer.toString(server % _pools)));
      enabled.put(id, new ServerInstance(config));
      instanceIds.add(id);
      id.hashCode();
    }
    IdealState ideal = new IdealState(TABLE);
    ideal.setReplicas(Integer.toString(REPLICAS));
    ideal.setNumPartitions(_segments);
    ideal.enable(true);
    ExternalView external = new ExternalView(TABLE);
    _segmentNames = new HashSet<>();
    _candidates = new HashMap<>();
    _fixedRequired = new HashMap<>();
    _fixedOptional = new HashMap<>();
    _optionalSegments = new HashSet<>();
    _unavailableSegments = new HashSet<>();
    Map<String, ZNRecord> newSegmentMetadata = new HashMap<>();
    for (int segment = 0; segment < _segments; segment++) {
      String name = String.format(Locale.ROOT, "routing_segment_%08d", segment);
      _segmentNames.add(name);
      Map<String, String> assignment = new TreeMap<>();
      for (int replica = 0; replica < REPLICAS; replica++) {
        assignment.put(instanceIds.get((segment + replica) % _servers), "ONLINE");
      }
      ideal.getRecord().setMapField(name, assignment);
      int percentage = (int) ((long) segment * 100 / _segments);
      boolean optional = percentage < _optionalPercent;
      boolean unavailable = percentage >= _optionalPercent && percentage < _optionalPercent + _unavailablePercent;
      Map<String, String> externalAssignment = new TreeMap<>();
      String externalState = optional ? "OFFLINE" : unavailable ? "ERROR" : "ONLINE";
      assignment.keySet().forEach(instance -> externalAssignment.put(instance, externalState));
      external.getRecord().setMapField(name, externalAssignment);
      _candidates.put(name, List.copyOf(assignment.keySet()));
      if (optional) {
        _optionalSegments.add(name);
        SegmentZKMetadata metadata = new SegmentZKMetadata(name);
        metadata.setCreationTime(FIXTURE_TIME_MS - 1000);
        newSegmentMetadata.put(name, metadata.toZNRecord());
      }
      if (unavailable) {
        _unavailableSegments.add(name);
      } else {
        (optional ? _fixedOptional : _fixedRequired).put(name, instanceIds.get(segment % _servers));
      }
    }
    DefaultSegmentSelector segmentSelector = new DefaultSegmentSelector();
    segmentSelector.init(ideal, external, _segmentNames);
    InstanceSelector selector;
    if (!_selector.equals("preselected")) {
      BaseInstanceSelector productionSelector = _selector.equals("balanced")
          ? new BalancedInstanceSelector() : new ReplicaGroupInstanceSelector();
      // Used during initialization only; real selectors interpret fresh OFFLINE and old ERROR replicas.
      ZkHelixPropertyStore<ZNRecord> store = Mockito.mock(ZkHelixPropertyStore.class,
          Mockito.withSettings().stubOnly().defaultAnswer(invocation -> {
            List<String> paths = invocation.getArgument(0);
            List<ZNRecord> records = new ArrayList<>(paths.size());
            for (String path : paths) {
              records.add(newSegmentMetadata.get(path.substring(path.lastIndexOf('/') + 1)));
            }
            return records;
          }));
      TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("routing_bench").build();
      Clock clock = Clock.fixed(Instant.ofEpochMilli(FIXTURE_TIME_MS), ZoneOffset.UTC);
      productionSelector.init(config, store, metrics, null, clock, new InstanceSelectorConfig(false, 300, false),
          enabled.keySet(), enabled, ideal, external, _segmentNames);
      selector = productionSelector;
    } else {
      selector = new FixedSelector(new InstanceSelector.InstanceMapping(_fixedRequired, _fixedOptional),
          List.copyOf(_unavailableSegments));
    }
    installRoutingEntry(segmentSelector, selector);
    _request = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setTableName(TABLE);
    _request.setQuerySource(querySource);
    for (long requestId : new long[]{0, 1, 2, 127, 128, 100000}) {
      validate(requestId);
    }
  }

  @SuppressWarnings("unchecked")
  private void installRoutingEntry(DefaultSegmentSelector segmentSelector, InstanceSelector instanceSelector)
      throws Exception {
    Class<?> entryClass = Class.forName(BaseBrokerRoutingManager.class.getName() + "$RoutingEntry");
    Constructor<?> constructor = entryClass.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    Object entry = constructor.newInstance(TABLE, "", "", null, segmentSelector, List.of(), instanceSelector,
        0, 0, null, null, null, null, Map.of(), false);
    Field field = BaseBrokerRoutingManager.class.getDeclaredField("_routingEntryMap");
    field.setAccessible(true);
    ((Map<String, Object>) field.get(_manager)).put(TABLE, entry);
  }

  private void validate(long requestId) {
    long[] beforeMeters = _poolMeters.stream().mapToLong(PinotMeter::count).toArray();
    RoutingTable routing = _manager.getRoutingTable(_request, requestId);
    if (routing == null || !new HashSet<>(routing.getUnavailableSegments()).equals(_unavailableSegments)
        || routing.getUnavailableSegments().size() != _unavailableSegments.size()
        || routing.getNumPrunedSegments() != 0) {
      throw new IllegalStateException("Unexpected missing/pruned routing result");
    }
    Map<String, String> expectedRequired = new HashMap<>();
    Map<String, String> expectedOptional = new HashMap<>();
    long[] expectedPoolCounts = new long[_pools];
    if (!_selector.equals("preselected")) {
      int current = (int) requestId;
      for (String segment : _segmentNames) {
        if (_unavailableSegments.contains(segment)) {
          continue;
        }
        String server = _candidates.get(segment).get(current % REPLICAS);
        (_optionalSegments.contains(segment) ? expectedOptional : expectedRequired).put(segment, server);
        expectedPoolCounts[_manager.getEnabledServerInstanceMap().get(server).getPool()]++;
        if (_selector.equals("balanced")) {
          current++;
        }
      }
    } else {
      expectedRequired.putAll(_fixedRequired);
      expectedOptional.putAll(_fixedOptional);
    }
    // Selected optional segments count in pool metrics even when grouping skips optional-only servers.
    for (int pool = 0; pool < _pools; pool++) {
      if (_poolMeters.get(pool).count() - beforeMeters[pool] != expectedPoolCounts[pool]) {
        throw new IllegalStateException("Incorrect observed count for pool " + pool);
      }
    }
    Set<String> activeServers = new HashSet<>(expectedRequired.values());
    expectedOptional.values().removeIf(server -> !activeServers.contains(server));
    Map<String, String> actualRequired = new HashMap<>();
    Map<String, String> actualOptional = new HashMap<>();
    for (Map.Entry<ServerInstance, SegmentsToQuery> entry : routing.getServerInstanceToSegmentsMap().entrySet()) {
      if (entry.getValue().getSegments().isEmpty()) {
        throw new IllegalStateException("Optional-only server must not be routed");
      }
      for (String segment : entry.getValue().getSegments()) {
        if (actualRequired.put(segment, entry.getKey().getInstanceId()) != null) {
          throw new IllegalStateException("Duplicate required segment");
        }
      }
      for (String segment : entry.getValue().getOptionalSegments()) {
        if (actualOptional.put(segment, entry.getKey().getInstanceId()) != null) {
          throw new IllegalStateException("Duplicate optional segment");
        }
      }
    }
    if (!actualRequired.equals(expectedRequired) || !actualOptional.equals(expectedOptional)) {
      throw new IllegalStateException("Routing differs from independently computed assignments");
    }
  }

  @TearDown
  public void tearDown() {
    _manager.stop();
    PinotMetricUtils.cleanUp();
  }

  @Benchmark
  public RoutingTable getRoutingTable(RequestState requestState) {
    return _manager.getRoutingTable(_request, requestState.next());
  }

  /// Each query worker owns its request counter; immutable routing state is shared across workers.
  @State(Scope.Thread)
  public static class RequestState {
    private long _requestId;

    long next() {
      return _requestId++;
    }
  }

  /// Returns a new SelectionResult wrapper around fixed maps; manager mutates its pruned count per request.
  private static final class FixedSelector implements InstanceSelector {
    private final InstanceMapping _mapping;
    private final List<String> _unavailable;

    FixedSelector(InstanceMapping mapping, List<String> unavailable) {
      _mapping = mapping;
      _unavailable = unavailable;
    }

    @Override
    public SelectionResult select(BrokerRequest request, List<String> segments, long requestId) {
      return new SelectionResult(_mapping, _unavailable, 0);
    }

    @Override
    public Set<String> getServingInstances() {
      return Set.copyOf(_mapping.segmentToInstanceMap().values());
    }

    @Override
    public void init(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics,
        AdaptiveServerSelector adaptiveServerSelector, Clock clock, InstanceSelectorConfig config,
        Set<String> enabledInstances, Map<String, ServerInstance> enabledServerMap, IdealState idealState,
        ExternalView externalView, Set<String> onlineSegments) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
      throw new UnsupportedOperationException();
    }
  }
}
