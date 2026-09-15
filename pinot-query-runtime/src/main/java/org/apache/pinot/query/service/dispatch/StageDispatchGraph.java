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
package org.apache.pinot.query.service.dispatch;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/// Dependency graph for dispatch groups. Streaming edges are dispatched together; materialized edges form barriers.
final class StageDispatchGraph {
  private final List<Group> _groups;
  private final Map<Set<Integer>, Group> _groupsByStages;

  private StageDispatchGraph(List<Group> groups) {
    _groups = groups;
    Map<Set<Integer>, Group> groupsByStages = new HashMap<>();
    for (Group group : groups) {
      groupsByStages.put(group._stages, group);
    }
    _groupsByStages = Map.copyOf(groupsByStages);
  }

  static StageDispatchGraph create(DispatchableSubPlan subPlan) {
    Map<Integer, DispatchablePlanFragment> stages = subPlan.getQueryStageMap();
    UnionFind unionFind = new UnionFind(stages.keySet());
    List<Edge> edges = collectEdges(stages);

    for (Edge edge : edges) {
      if (!edge._materialized) {
        if (edge._exchangeType != PinotRelExchangeType.STREAMING
            && edge._exchangeType != PinotRelExchangeType.PIPELINE_BREAKER) {
          throw new IllegalArgumentException("Unsupported stage exchange type: " + edge._exchangeType);
        }
        unionFind.union(edge._senderStageId, edge._receiverStageId);
      }
    }

    Map<Integer, Set<Integer>> stagesByRoot = new TreeMap<>();
    for (int stageId : new TreeSet<>(stages.keySet())) {
      stagesByRoot.computeIfAbsent(unionFind.find(stageId), ignored -> new TreeSet<>()).add(stageId);
    }

    List<Group> groups = new ArrayList<>(stagesByRoot.size());
    Map<Integer, Group> groupByStage = new HashMap<>();
    for (Set<Integer> stageIds : stagesByRoot.values()) {
      Group group = new Group(stageIds);
      groups.add(group);
      for (int stageId : stageIds) {
        groupByStage.put(stageId, group);
      }
    }

    for (Edge edge : edges) {
      if (edge._materialized) {
        Group sender = groupByStage.get(edge._senderStageId);
        Group receiver = groupByStage.get(edge._receiverStageId);
        if (sender == receiver) {
          throw new IllegalArgumentException("Materialized dependency forms a cycle within group " + sender._stages);
        }
        receiver._dependencies.add(sender);
      }
    }
    rejectCycles(groups);
    return new StageDispatchGraph(List.copyOf(groups));
  }

  synchronized List<Set<Integer>> getReadyGroups() {
    List<Set<Integer>> ready = new ArrayList<>();
    for (Group group : _groups) {
      if (!group._dispatched && dependenciesComplete(group)) {
        ready.add(group._stages);
      }
    }
    return List.copyOf(ready);
  }

  synchronized void markDispatched(Set<Integer> stages) {
    Group group = getGroup(stages);
    if (group._dispatched) {
      throw new IllegalStateException("Dispatch group already dispatched: " + stages);
    }
    if (!dependenciesComplete(group)) {
      throw new IllegalStateException("Dispatch group is not ready: " + stages);
    }
    group._dispatched = true;
  }

  synchronized void markCompleted(Set<Integer> stages) {
    Group group = getGroup(stages);
    if (!group._dispatched) {
      throw new IllegalStateException("Dispatch group was not dispatched: " + stages);
    }
    if (group._completed) {
      throw new IllegalStateException("Dispatch group already completed: " + stages);
    }
    group._completed = true;
  }

  synchronized boolean isComplete() {
    for (Group group : _groups) {
      if (!group._completed) {
        return false;
      }
    }
    return true;
  }

  private Group getGroup(Set<Integer> stages) {
    Group group = _groupsByStages.get(stages);
    if (group == null) {
      throw new IllegalArgumentException("Unknown dispatch group: " + stages);
    }
    return group;
  }

  private static boolean dependenciesComplete(Group group) {
    for (Group dependency : group._dependencies) {
      if (!dependency._completed) {
        return false;
      }
    }
    return true;
  }

  private static List<Edge> collectEdges(Map<Integer, DispatchablePlanFragment> stages) {
    List<Edge> edges = new ArrayList<>();
    for (int stageId : new TreeSet<>(stages.keySet())) {
      PlanNode root = stages.get(stageId).getPlanFragment().getFragmentRoot();
      Deque<PlanNode> pending = new ArrayDeque<>();
      pending.push(root);
      while (!pending.isEmpty()) {
        PlanNode node = pending.pop();
        if (node instanceof MailboxReceiveNode) {
          MailboxReceiveNode receive = (MailboxReceiveNode) node;
          int senderStageId = receive.getSenderStageId();
          if (!stages.containsKey(senderStageId)) {
            throw new IllegalArgumentException("Unknown sender stage: " + senderStageId);
          }
          edges.add(new Edge(senderStageId, stageId, receive.getExchangeType(), receive.isMaterialized()));
        }
        List<PlanNode> inputs = node.getInputs();
        for (int i = inputs.size() - 1; i >= 0; i--) {
          pending.push(inputs.get(i));
        }
      }
    }
    return edges;
  }

  private static void rejectCycles(List<Group> groups) {
    Map<Group, Integer> remainingDependencies = new HashMap<>();
    Map<Group, List<Group>> dependents = new HashMap<>();
    PriorityQueue<Group> ready = new PriorityQueue<>((left, right) ->
        Integer.compare(left._stages.iterator().next(), right._stages.iterator().next()));
    for (Group group : groups) {
      int dependencyCount = group._dependencies.size();
      remainingDependencies.put(group, dependencyCount);
      if (dependencyCount == 0) {
        ready.add(group);
      }
      for (Group dependency : group._dependencies) {
        dependents.computeIfAbsent(dependency, ignored -> new ArrayList<>()).add(group);
      }
    }

    int visited = 0;
    while (!ready.isEmpty()) {
      Group group = ready.remove();
      visited++;
      for (Group dependent : dependents.getOrDefault(group, List.of())) {
        int remaining = remainingDependencies.compute(dependent, (ignored, count) -> count - 1);
        if (remaining == 0) {
          ready.add(dependent);
        }
      }
    }
    if (visited != groups.size()) {
      throw new IllegalArgumentException("Stage dispatch graph contains a cycle");
    }
  }

  private static final class Group {
    private final Set<Integer> _stages;
    private final Set<Group> _dependencies = new HashSet<>();
    private boolean _dispatched;
    private boolean _completed;

    private Group(Set<Integer> stages) {
      _stages = Collections.unmodifiableSet(new TreeSet<>(stages));
    }
  }

  private static final class Edge {
    private final int _senderStageId;
    private final int _receiverStageId;
    private final PinotRelExchangeType _exchangeType;
    private final boolean _materialized;

    private Edge(int senderStageId, int receiverStageId, PinotRelExchangeType exchangeType, boolean materialized) {
      _senderStageId = senderStageId;
      _receiverStageId = receiverStageId;
      _exchangeType = exchangeType;
      _materialized = materialized;
    }
  }

  private static final class UnionFind {
    private final Map<Integer, Integer> _parents = new HashMap<>();

    private UnionFind(Set<Integer> stageIds) {
      for (int stageId : stageIds) {
        _parents.put(stageId, stageId);
      }
    }

    private int find(int stageId) {
      Integer parent = _parents.get(stageId);
      if (parent == null) {
        throw new IllegalArgumentException("Unknown stage: " + stageId);
      }
      if (parent != stageId) {
        parent = find(parent);
        _parents.put(stageId, parent);
      }
      return parent;
    }

    private void union(int firstStageId, int secondStageId) {
      int firstRoot = find(firstStageId);
      int secondRoot = find(secondStageId);
      if (firstRoot != secondRoot) {
        int root = Math.min(firstRoot, secondRoot);
        _parents.put(firstRoot, root);
        _parents.put(secondRoot, root);
      }
    }
  }
}
