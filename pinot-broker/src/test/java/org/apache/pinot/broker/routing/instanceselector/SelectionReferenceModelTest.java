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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.adaptiveserverselector.PriorityPoolInstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector.SelectionResult;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.broker.routing.instanceselector.SegmentStatesTestUtils.createSegmentStates;
import static org.apache.pinot.broker.routing.instanceselector.SegmentStatesTestUtils.getCandidates;
import static org.apache.pinot.broker.routing.instanceselector.SelectionResultTestUtils.getOptionalSegmentToInstanceMap;
import static org.apache.pinot.broker.routing.instanceselector.SelectionResultTestUtils.getSegmentToInstanceMap;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.FALLBACK_POOL_ID;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;


/// Checks that each instance selector selects exactly what a straightforward per-segment reference model selects, over
/// random segment states and random queries.
///
/// The reference models iterate the queried segments in query order, and pick one of the candidates returned by
/// [SegmentStatesTestUtils#getCandidates] for each segment. A segment selected on an offline candidate is optional. A
/// queried segment without candidate is unavailable when it is an old segment, and skipped when it is unknown. The
/// reference models also count the selected segments of each pool, which the selectors emit as `POOL_SEG_QUERIES`.
///
/// Each run also checks that its random segment states, queries and rankings reach the [Scenario]s expected for its
/// parameters (see [#getExpectedScenarios]).
public class SelectionReferenceModelTest {
  private static final int NUM_SEEDS = 50;
  private static final int MAX_NUM_INSTANCES = 6;
  private static final int MAX_NUM_SEGMENTS = 40;
  private static final int[] POOLS = {FALLBACK_POOL_ID, 0, 1, 128};
  private static final long NEW_SEGMENT_EXPIRATION_SECONDS = 300;
  private static final QueryParams NO_QUERY_OPTIONS = new QueryParams(null, null, List.of());

  @DataProvider(name = "selectionCases")
  public Object[][] selectionCases() {
    List<QueryParams> queryParamsCases = new ArrayList<>();
    queryParamsCases.add(NO_QUERY_OPTIONS);
    queryParamsCases.add(new QueryParams(null, true, List.of()));
    for (int numReplicaGroupsToQuery = 1; numReplicaGroupsToQuery <= 4; numReplicaGroupsToQuery++) {
      queryParamsCases.add(new QueryParams(numReplicaGroupsToQuery, null, List.of()));
      queryParamsCases.add(new QueryParams(numReplicaGroupsToQuery, true, List.of()));
    }
    queryParamsCases.add(new QueryParams(null, null, List.of(128, 0)));
    queryParamsCases.add(new QueryParams(2, null, List.of(1, FALLBACK_POOL_ID)));
    queryParamsCases.add(new QueryParams(null, true, List.of(128, 0)));

    List<Object[]> cases = new ArrayList<>();
    for (SelectorType selectorType : SelectorType.values()) {
      for (QueryParams queryParams : queryParamsCases) {
        cases.add(new Object[]{selectorType, false, false, queryParams});
        cases.add(new Object[]{selectorType, true, false, queryParams});
      }
      // Fixed replica routing in the config cannot be combined with adaptive server selection
      cases.add(new Object[]{selectorType, false, true, NO_QUERY_OPTIONS});
      cases.add(new Object[]{selectorType, false, true, new QueryParams(2, null, List.of())});
      cases.add(new Object[]{selectorType, false, true, new QueryParams(null, false, List.of())});
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "selectionCases")
  public void testSelectionMatchesReferenceModel(SelectorType selectorType, boolean adaptive,
      boolean useFixedReplicaInConfig, QueryParams queryParams) {
    InstanceSelectorConfig config =
        new InstanceSelectorConfig(useFixedReplicaInConfig, NEW_SEGMENT_EXPIRATION_SECONDS, false);
    Map<String, String> queryOptions = queryParams.toQueryOptions();
    String preferredPoolTag = BrokerMetrics.getTagForPreferredPool(queryOptions);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    Set<Scenario> reachedScenarios = EnumSet.noneOf(Scenario.class);
    for (int seed = 0; seed < NUM_SEEDS; seed++) {
      Random random = new Random(seed);
      RandomState state = createRandomState(random);
      // As the hash of the raw table name: non-negative and below the cap
      int tableNameHash = (random.nextInt() & Integer.MAX_VALUE) % BaseInstanceSelector.MAX_TABLE_NAME_HASH;
      Ranking ranking = adaptive ? createRandomRanking(random, state.instances()) : null;
      SelectorParams params = new SelectorParams(config, tableNameHash, ranking);
      BaseInstanceSelector selector = createSelector(selectorType, params, brokerMetrics);
      for (int requestId : getRequestIds(random)) {
        List<String> segments = createRandomQuery(random, state.segments());
        SelectionResult selectionResult = selector.select(segments, requestId, state.segmentStates(), queryOptions);
        ExpectedSelection expectedSelection =
            selectorType._referenceModel.select(segments, requestId, state, params, queryParams);
        String context = "seed: " + seed + ", requestId: " + requestId + ", segments: " + segments;
        assertSelection(selectionResult, expectedSelection, segments, context);
        for (Map.Entry<Integer, Integer> entry : expectedSelection._numSegmentsByPool.entrySet()) {
          verify(brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, entry.getValue().longValue(),
              preferredPoolTag, String.valueOf(entry.getKey()));
        }
        verifyNoMoreInteractions(brokerMetrics);
        clearInvocations(brokerMetrics);
        reachedScenarios.addAll(expectedSelection._scenarios);
      }
    }
    assertEquals(reachedScenarios, getExpectedScenarios(selectorType, adaptive, config, queryParams));
  }

  /// Returns the scenarios that the selections of a run reach: every run selects several segments of one group,
  /// selects optional segments and reports unavailable segments, while fixed replica routing and the random rankings
  /// are only reached when the reference model of the selector uses them for the run parameters.
  private static Set<Scenario> getExpectedScenarios(SelectorType selectorType, boolean adaptive,
      InstanceSelectorConfig config, QueryParams queryParams) {
    Set<Scenario> expectedScenarios =
        EnumSet.of(Scenario.SHARED_GROUP, Scenario.OPTIONAL_SELECTION, Scenario.UNAVAILABLE_SEGMENT);
    boolean useFixedReplica = queryParams.isUseFixedReplica(config);
    if (selectorType == SelectorType.BALANCED) {
      // The adaptive server selection of the balanced selector ignores fixed replica routing
      if (useFixedReplica && !adaptive) {
        expectedScenarios.add(Scenario.FIXED_REPLICA);
      }
    } else if (useFixedReplica) {
      expectedScenarios.add(Scenario.FIXED_REPLICA);
    } else if (adaptive) {
      // The strict replica-group selector does not rank the instances when the number of replica groups to query is set
      if (selectorType == SelectorType.REPLICA_GROUP || queryParams.numReplicaGroupsToQuery() == null) {
        expectedScenarios.addAll(List.of(Scenario.RANKED_SELECTION, Scenario.PARTIAL_RANKING, Scenario.EMPTY_RANKING));
      }
    }
    return expectedScenarios;
  }

  /// Asserts that the selection result matches the expected selection, and that the segments of each selected instance
  /// are in query order.
  private static void assertSelection(SelectionResult selectionResult, ExpectedSelection expectedSelection,
      List<String> segments, String context) {
    assertEquals(getSegmentToInstanceMap(selectionResult), expectedSelection._segmentToInstanceMap, context);
    assertEquals(getOptionalSegmentToInstanceMap(selectionResult), expectedSelection._optionalSegmentToInstanceMap,
        context);
    assertEquals(selectionResult.getUnavailableSegments(), expectedSelection._unavailableSegments, context);
    Map<String, SegmentsToQuery> instanceToSegmentsMap = selectionResult.getInstanceToSegmentsMap();
    Set<String> expectedInstances = new HashSet<>(expectedSelection._segmentToInstanceMap.values());
    expectedInstances.addAll(expectedSelection._optionalSegmentToInstanceMap.values());
    assertEquals(instanceToSegmentsMap.keySet(), expectedInstances, context);
    for (SegmentsToQuery segmentsToQuery : instanceToSegmentsMap.values()) {
      assertInQueryOrder(segmentsToQuery.getSegments(), segments, context);
      assertInQueryOrder(segmentsToQuery.getOptionalSegments(), segments, context);
    }
  }

  /// Asserts that the given segments of one instance are in the order of the queried segments.
  private static void assertInQueryOrder(List<String> instanceSegments, List<String> segments, String context) {
    Set<String> instanceSegmentSet = new HashSet<>(instanceSegments);
    List<String> expectedInstanceSegments = new ArrayList<>(instanceSegments.size());
    for (String segment : segments) {
      if (instanceSegmentSet.contains(segment)) {
        expectedInstanceSegments.add(segment);
      }
    }
    assertEquals(instanceSegments, expectedInstanceSegments, context);
  }

  /// Returns random segment states over up to [#MAX_NUM_INSTANCES] instances, each in a fixed pool. Most segments share
  /// one of a few candidate lists, and so a candidate group, the others have their own. A quarter of the candidate
  /// lists are of new segments, with some offline candidates. A tenth of the segments are unavailable. The segments a
  /// query can ask for also include a few unknown segments.
  private static RandomState createRandomState(Random random) {
    int numInstances = 1 + random.nextInt(MAX_NUM_INSTANCES);
    List<String> instances = new ArrayList<>(numInstances);
    int[] pools = new int[numInstances];
    for (int i = 0; i < numInstances; i++) {
      instances.add("server_" + i);
      pools[i] = POOLS[random.nextInt(POOLS.length)];
    }
    int numSharedCandidateLists = 1 + random.nextInt(3);
    List<List<SegmentInstanceCandidate>> sharedCandidateLists = new ArrayList<>(numSharedCandidateLists);
    for (int i = 0; i < numSharedCandidateLists; i++) {
      sharedCandidateLists.add(createRandomCandidates(random, instances, pools));
    }

    int numSegments = 1 + random.nextInt(MAX_NUM_SEGMENTS);
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    Set<String> unavailableSegments = new HashSet<>();
    List<String> segments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      String segment = "segment_" + i;
      segments.add(segment);
      int kind = random.nextInt(10);
      if (kind == 0) {
        unavailableSegments.add(segment);
      } else if (kind < 7) {
        candidatesMap.put(segment, sharedCandidateLists.get(random.nextInt(numSharedCandidateLists)));
      } else {
        candidatesMap.put(segment, createRandomCandidates(random, instances, pools));
      }
    }
    int numUnknownSegments = random.nextInt(4);
    for (int i = 0; i < numUnknownSegments; i++) {
      segments.add("unknown_" + i);
    }
    return new RandomState(createSegmentStates(candidatesMap, unavailableSegments), instances, unavailableSegments,
        segments);
  }

  /// Returns a non-empty random subset of the given instances as candidates, in instance order. A quarter of the lists
  /// are of new segments, where each candidate is offline with a probability of 1/2.
  private static List<SegmentInstanceCandidate> createRandomCandidates(Random random, List<String> instances,
      int[] pools) {
    boolean newSegment = random.nextInt(4) == 0;
    List<SegmentInstanceCandidate> candidates = new ArrayList<>();
    while (candidates.isEmpty()) {
      for (int i = 0; i < instances.size(); i++) {
        if (random.nextBoolean()) {
          boolean online = !newSegment || random.nextBoolean();
          candidates.add(new SegmentInstanceCandidate(instances.get(i), online, pools[i], candidates.size()));
        }
      }
    }
    return candidates;
  }

  /// Returns a random subset of the given segments, in random order.
  private static List<String> createRandomQuery(Random random, List<String> segments) {
    List<String> shuffledSegments = new ArrayList<>(segments);
    Collections.shuffle(shuffledSegments, random);
    return new ArrayList<>(shuffledSegments.subList(0, random.nextInt(segments.size() + 1)));
  }

  /// Returns a random ranking of the given instances, which leaves no instance, every instance, or random instances
  /// unranked, so that the adaptive server selection gets full, empty and partial rankings.
  private static Ranking createRandomRanking(Random random, List<String> instances) {
    List<String> order = new ArrayList<>(instances);
    Collections.shuffle(order, random);
    int mode = random.nextInt(4);
    Set<String> omitted = new HashSet<>();
    for (String instance : instances) {
      if (mode == 1 || (mode > 1 && random.nextInt(3) == 0)) {
        omitted.add(instance);
      }
    }
    return new Ranking(order, omitted);
  }

  /// Returns the request ids to select with: the broker request ids are non-negative, and [BaseInstanceSelector] passes
  /// them modulo [BaseInstanceSelector#MAX_REQUEST_ID].
  private static int[] getRequestIds(Random random) {
    int maxRequestId = (int) BaseInstanceSelector.MAX_REQUEST_ID;
    return new int[]{0, 1, 2, maxRequestId - 1, random.nextInt(maxRequestId), random.nextInt(maxRequestId)};
  }

  /// Returns a selector of the given type with the given parameters and broker metrics.
  private static BaseInstanceSelector createSelector(SelectorType selectorType, SelectorParams params,
      BrokerMetrics brokerMetrics) {
    BaseInstanceSelector selector = selectorType._selectorFactory.get();
    selector._brokerMetrics = brokerMetrics;
    selector._config = params.config();
    selector._tableNameHashForFixedReplicaRouting = params.tableNameHash();
    if (params.ranking() != null) {
      selector._adaptiveServerSelector = mockAdaptiveServerSelector(params.ranking());
      selector._priorityPoolInstanceSelector = new PriorityPoolInstanceSelector(selector._adaptiveServerSelector);
    }
    return selector;
  }

  /// Returns an adaptive server selector that ranks and selects the instances by the given ranking.
  private static AdaptiveServerSelector mockAdaptiveServerSelector(Ranking ranking) {
    AdaptiveServerSelector adaptiveServerSelector = mock(AdaptiveServerSelector.class);
    when(adaptiveServerSelector.fetchServerRankingsWithScores(anyList())).thenAnswer(invocation -> {
      List<Pair<String, Double>> rankingWithScores = new ArrayList<>();
      for (String instance : ranking.rank(invocation.getArgument(0))) {
        rankingWithScores.add(Pair.of(instance, (double) rankingWithScores.size()));
      }
      return rankingWithScores;
    });
    when(adaptiveServerSelector.select(anyList())).thenAnswer(
        invocation -> ranking.selectBest(invocation.getArgument(0)));
    return adaptiveServerSelector;
  }

  /// Returns the selection of the reference model of [BalancedInstanceSelector], which selects each segment on:
  /// - the candidate picked by the adaptive server selection, when it is enabled (see [#selectByPoolAndRanking])
  /// - else the candidate `tableNameHash % numCandidates`, with fixed replica routing
  /// - else the candidate `requestId % numCandidates`, where the request id is incremented for each routable segment
  private static ExpectedSelection selectBalanced(List<String> segments, int requestId, RandomState state,
      SelectorParams params, QueryParams queryParams) {
    ExpectedSelection expectedSelection = new ExpectedSelection();
    boolean useFixedReplica = queryParams.isUseFixedReplica(params.config());
    int nextRequestId = requestId;
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = expectedSelection.resolveCandidates(segment, state);
      if (candidates == null) {
        continue;
      }
      SegmentInstanceCandidate selectedCandidate;
      if (params.ranking() != null) {
        selectedCandidate = selectByPoolAndRanking(candidates, params.ranking(), queryParams.preferredPools());
      } else if (useFixedReplica) {
        selectedCandidate = candidates.get(params.tableNameHash() % candidates.size());
        expectedSelection._scenarios.add(Scenario.FIXED_REPLICA);
      } else {
        selectedCandidate = candidates.get(nextRequestId++ % candidates.size());
      }
      expectedSelection.add(segment, selectedCandidate);
    }
    return expectedSelection;
  }

  /// Returns the candidate picked by the per-segment adaptive server selection: among the candidates in the pool with
  /// the highest priority, the best one by the ranking, ranked or not.
  private static SegmentInstanceCandidate selectByPoolAndRanking(List<SegmentInstanceCandidate> candidates,
      Ranking ranking, List<Integer> preferredPools) {
    Comparator<SegmentInstanceCandidate> byPoolPriority =
        Comparator.comparingInt(candidate -> getPoolPriority(preferredPools, candidate.getPool()));
    return candidates.stream()
        .min(byPoolPriority.thenComparingInt(candidate -> ranking.order().indexOf(candidate.getInstance())))
        .orElseThrow();
  }

  /// Returns the selection of the reference model of [ReplicaGroupInstanceSelector]. The segments rotate over
  /// `numReplicaGroupsToQuery` replica groups (1 when not set) through a replica offset, which wraps at the smallest
  /// number of candidates seen so far. Each segment is selected on:
  /// - the candidate `floorMod(tableNameHash + replicaOffset, numCandidates)`, with fixed replica routing
  /// - else the best ranked candidate, when the adaptive server selection is enabled and ranks any instance (see
  ///   [#getReferenceRanks]), unless a candidate is unranked, in which case it falls back to the round robin
  ///   candidate
  /// - else the round robin candidate `floorMod(requestId + replicaOffset, numCandidates)`
  private static ExpectedSelection selectReplicaGroup(List<String> segments, int requestId, RandomState state,
      SelectorParams params, QueryParams queryParams) {
    ExpectedSelection expectedSelection = new ExpectedSelection();
    boolean useFixedReplica = queryParams.isUseFixedReplica(params.config());
    Map<String, Integer> ranks = params.ranking() != null
        ? getReferenceRanks(segments, state.segmentStates(), params.ranking(), queryParams.preferredPools())
        : Map.of();
    int numReplicaGroups = queryParams.numReplicaGroupsToQuery() != null ? queryParams.numReplicaGroupsToQuery() : 1;
    int replicaOffset = 0;
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = expectedSelection.resolveCandidates(segment, state);
      if (candidates == null) {
        continue;
      }
      int numCandidates = candidates.size();
      SegmentInstanceCandidate roundRobinCandidate =
          candidates.get(Math.floorMod(requestId + replicaOffset, numCandidates));
      SegmentInstanceCandidate selectedCandidate;
      if (useFixedReplica) {
        selectedCandidate = candidates.get(Math.floorMod(params.tableNameHash() + replicaOffset, numCandidates));
        expectedSelection._scenarios.add(Scenario.FIXED_REPLICA);
      } else if (params.ranking() == null) {
        selectedCandidate = roundRobinCandidate;
      } else if (ranks.isEmpty()) {
        selectedCandidate = roundRobinCandidate;
        expectedSelection._scenarios.add(Scenario.EMPTY_RANKING);
      } else {
        selectedCandidate = getBestRankedCandidate(candidates, ranks);
        if (selectedCandidate != null) {
          expectedSelection._scenarios.add(Scenario.RANKED_SELECTION);
        } else {
          selectedCandidate = roundRobinCandidate;
          expectedSelection._scenarios.add(Scenario.PARTIAL_RANKING);
        }
      }
      expectedSelection.add(segment, selectedCandidate);
      numReplicaGroups = Math.min(numReplicaGroups, numCandidates);
      replicaOffset = (replicaOffset + 1) % numReplicaGroups;
    }
    return expectedSelection;
  }

  /// Returns the selection of the reference model of [StrictReplicaGroupInstanceSelector]: the replica-group reference
  /// model, where the queries with fixed replica routing or with a number of replica groups to query do not use the
  /// adaptive server selection.
  private static ExpectedSelection selectStrictReplicaGroup(List<String> segments, int requestId, RandomState state,
      SelectorParams params, QueryParams queryParams) {
    boolean nonAdaptive =
        queryParams.isUseFixedReplica(params.config()) || queryParams.numReplicaGroupsToQuery() != null;
    return selectReplicaGroup(segments, requestId, state, nonAdaptive ? params.withoutRanking() : params, queryParams);
  }

  /// Returns the rank of each ranked instance in the one ranking of a query: the instances of the candidates of the
  /// queried segments, ranked by the given ranking, then ordered by the priority of their pools.
  private static Map<String, Integer> getReferenceRanks(List<String> segments, SegmentStates segmentStates,
      Ranking ranking, List<Integer> preferredPools) {
    Map<String, Integer> poolByInstance = new HashMap<>();
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = getCandidates(segmentStates, segment);
      if (candidates != null) {
        for (SegmentInstanceCandidate candidate : candidates) {
          poolByInstance.put(candidate.getInstance(), candidate.getPool());
        }
      }
    }
    List<String> rankedInstances = ranking.rank(poolByInstance.keySet());
    rankedInstances.sort(
        Comparator.comparingInt(instance -> getPoolPriority(preferredPools, poolByInstance.get(instance))));
    Map<String, Integer> ranks = new HashMap<>();
    for (int rank = 0; rank < rankedInstances.size(); rank++) {
      ranks.put(rankedInstances.get(rank), rank);
    }
    return ranks;
  }

  /// Returns the best ranked candidate, or `null` when any candidate is unranked.
  @Nullable
  private static SegmentInstanceCandidate getBestRankedCandidate(List<SegmentInstanceCandidate> candidates,
      Map<String, Integer> ranks) {
    SegmentInstanceCandidate bestCandidate = null;
    int bestRank = Integer.MAX_VALUE;
    for (SegmentInstanceCandidate candidate : candidates) {
      Integer rank = ranks.get(candidate.getInstance());
      if (rank == null) {
        return null;
      }
      if (rank < bestRank) {
        bestRank = rank;
        bestCandidate = candidate;
      }
    }
    return bestCandidate;
  }

  /// Returns the priority of the given pool, lower is preferred: its position in the preferred pools, or a priority
  /// after every preferred pool when it is not preferred.
  private static int getPoolPriority(List<Integer> preferredPools, int pool) {
    int index = preferredPools.indexOf(pool);
    return index >= 0 ? index : preferredPools.size();
  }

  /// The selectors under test, along with their reference models.
  enum SelectorType {
    BALANCED(BalancedInstanceSelector::new, SelectionReferenceModelTest::selectBalanced),
    REPLICA_GROUP(ReplicaGroupInstanceSelector::new, SelectionReferenceModelTest::selectReplicaGroup),
    STRICT_REPLICA_GROUP(StrictReplicaGroupInstanceSelector::new,
        SelectionReferenceModelTest::selectStrictReplicaGroup);

    final Supplier<BaseInstanceSelector> _selectorFactory;
    final ReferenceModel _referenceModel;

    SelectorType(Supplier<BaseInstanceSelector> selectorFactory, ReferenceModel referenceModel) {
      _selectorFactory = selectorFactory;
      _referenceModel = referenceModel;
    }
  }

  /// A reference model, which returns the expected selection of the given segments.
  @FunctionalInterface
  private interface ReferenceModel {
    ExpectedSelection select(List<String> segments, int requestId, RandomState state, SelectorParams params,
        QueryParams queryParams);
  }

  /// The selection query options of a query, `null` or empty when not set.
  record QueryParams(@Nullable Integer numReplicaGroupsToQuery, @Nullable Boolean useFixedReplica,
      List<Integer> preferredPools) {
    Map<String, String> toQueryOptions() {
      Map<String, String> queryOptions = new HashMap<>();
      if (numReplicaGroupsToQuery != null) {
        queryOptions.put(QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY, numReplicaGroupsToQuery.toString());
      }
      if (useFixedReplica != null) {
        queryOptions.put(QueryOptionKey.USE_FIXED_REPLICA, useFixedReplica.toString());
      }
      if (!preferredPools.isEmpty()) {
        queryOptions.put(QueryOptionKey.ORDERED_PREFERRED_POOLS,
            preferredPools.stream().map(String::valueOf).collect(Collectors.joining("|")));
      }
      return queryOptions;
    }

    /// Returns whether the query uses fixed replica routing: the query option when set, else the config.
    boolean isUseFixedReplica(InstanceSelectorConfig config) {
      return useFixedReplica != null ? useFixedReplica : config.isUseFixedReplica();
    }
  }

  /// The parameters of a selector: its config, the hash of its raw table name for fixed replica routing, and the
  /// ranking of its adaptive server selector, or `null` when the adaptive server selection is disabled.
  private record SelectorParams(InstanceSelectorConfig config, int tableNameHash, @Nullable Ranking ranking) {
    /// Returns the same parameters with the adaptive server selection disabled.
    SelectorParams withoutRanking() {
      return new SelectorParams(config, tableNameHash, null);
    }
  }

  /// Random segment states, along with their instances, their unavailable segments, and the segments a query can ask
  /// for, which include unknown segments.
  private record RandomState(SegmentStates segmentStates, List<String> instances, Set<String> unavailableSegments,
      List<String> segments) {
  }

  /// A server ranking that orders the instances as `order`, and leaves the `omitted` instances unranked.
  private record Ranking(List<String> order, Set<String> omitted) {
    /// Returns the ranked instances among the given instances, best first.
    List<String> rank(Collection<String> instances) {
      List<String> rankedInstances = new ArrayList<>();
      for (String instance : order) {
        if (instances.contains(instance) && !omitted.contains(instance)) {
          rankedInstances.add(instance);
        }
      }
      return rankedInstances;
    }

    /// Returns the best of the given instances by the order, ranked or not.
    String selectBest(List<String> instances) {
      for (String instance : order) {
        if (instances.contains(instance)) {
          return instance;
        }
      }
      throw new IllegalArgumentException("None of the instances: " + instances + " is in the order: " + order);
    }
  }

  /// Scenarios that the selections of a run reach, see [#getExpectedScenarios].
  private enum Scenario {
    /// Several queried segments of one candidate group are selected.
    SHARED_GROUP,
    /// A queried segment is selected on an offline candidate, as an optional segment.
    OPTIONAL_SELECTION,
    /// A queried unavailable segment is reported.
    UNAVAILABLE_SEGMENT,
    /// A segment is selected with fixed replica routing.
    FIXED_REPLICA,
    /// The replica-group selection selects the best ranked candidate of a segment.
    RANKED_SELECTION,
    /// The replica-group selection falls back to the round robin candidate of a segment with an unranked candidate.
    PARTIAL_RANKING,
    /// The replica-group selection selects the round robin candidate, as the ranking ranks none of the queried
    /// instances.
    EMPTY_RANKING
  }

  /// The selection of a reference model, along with the scenarios it reaches.
  private static final class ExpectedSelection {
    final Map<String, String> _segmentToInstanceMap = new HashMap<>();
    final Map<String, String> _optionalSegmentToInstanceMap = new HashMap<>();
    final List<String> _unavailableSegments = new ArrayList<>();
    final Map<Integer, Integer> _numSegmentsByPool = new HashMap<>();
    final Set<Scenario> _scenarios = EnumSet.noneOf(Scenario.class);
    // Groups of the segments with candidates
    private final Set<Integer> _groupIds = new HashSet<>();

    /// Returns the candidates of the given segment, or `null` when it has none, in which case the segment is recorded
    /// as unavailable when it is an unavailable segment of the state.
    @Nullable
    List<SegmentInstanceCandidate> resolveCandidates(String segment, RandomState state) {
      SegmentStates segmentStates = state.segmentStates();
      List<SegmentInstanceCandidate> candidates = getCandidates(segmentStates, segment);
      if (candidates != null) {
        if (!_groupIds.add(segmentStates.getGroupId(segment))) {
          _scenarios.add(Scenario.SHARED_GROUP);
        }
      } else if (state.unavailableSegments().contains(segment)) {
        _unavailableSegments.add(segment);
        _scenarios.add(Scenario.UNAVAILABLE_SEGMENT);
      }
      return candidates;
    }

    /// Selects the given segment on the given candidate, as an optional segment when the candidate is offline.
    void add(String segment, SegmentInstanceCandidate candidate) {
      if (candidate.isOnline()) {
        _segmentToInstanceMap.put(segment, candidate.getInstance());
      } else {
        _optionalSegmentToInstanceMap.put(segment, candidate.getInstance());
        _scenarios.add(Scenario.OPTIONAL_SELECTION);
      }
      _numSegmentsByPool.merge(candidate.getPool(), 1, Integer::sum);
    }
  }
}
