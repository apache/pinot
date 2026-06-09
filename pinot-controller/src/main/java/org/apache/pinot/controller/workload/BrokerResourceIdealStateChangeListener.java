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
package org.apache.pinot.controller.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.IdealState;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Listens for BrokerResource IdealState changes and notifies the workload manager for tables led by this controller.
///
/// Helix can invoke listener callbacks concurrently. This class serializes callbacks so the observed assignment
/// snapshot and pending per-table changes form one atomic state transition.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class BrokerResourceIdealStateChangeListener implements IdealStateChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceIdealStateChangeListener.class);

  private final QueryWorkloadManager _queryWorkloadManager;
  private final LeadControllerManager _leadControllerManager;
  private final Supplier<IdealState> _brokerResourceSupplier;
  private final Supplier<Set<String>> _tableNamesSupplier;
  private final Set<String> _pendingAddedTables = new TreeSet<>();
  private final Set<String> _pendingRemovedTables = new TreeSet<>();

  private Map<String, Map<String, String>> _observedBrokerAssignments = Map.of();
  private boolean _initialized;

  public BrokerResourceIdealStateChangeListener(QueryWorkloadManager queryWorkloadManager,
      LeadControllerManager leadControllerManager, Supplier<IdealState> brokerResourceSupplier,
      Supplier<Set<String>> tableNamesSupplier) {
    _queryWorkloadManager = queryWorkloadManager;
    _leadControllerManager = leadControllerManager;
    _brokerResourceSupplier = brokerResourceSupplier;
    _tableNamesSupplier = tableNamesSupplier;
  }

  @Override
  public synchronized void onIdealStateChange(List<IdealState> idealStates, NotificationContext context) {
    NotificationContext.Type type = context.getType();
    if (type == NotificationContext.Type.FINALIZE) {
      return;
    }
    if (type == NotificationContext.Type.CALLBACK && !isBrokerResourceChange(context)) {
      return;
    }
    if (type != NotificationContext.Type.INIT && type != NotificationContext.Type.CALLBACK
        && type != NotificationContext.Type.PERIODIC_REFRESH) {
      return;
    }

    reconcileCurrentAssignments();
  }

  /// Reconciles changes that were retained while this controller was not the leader for the affected tables.
  public synchronized void onLeadershipAcquired() {
    reconcileCurrentAssignments();
  }

  private void reconcileCurrentAssignments() {
    try {
      Map<String, Map<String, String>> currentBrokerAssignments =
          getBrokerAssignments(_brokerResourceSupplier.get());
      if (_initialized) {
        recordChanges(_observedBrokerAssignments, currentBrokerAssignments);
      } else {
        // The workload manager might have started after BrokerResource was populated, so INIT (or an earlier
        // leadership callback) must reconcile every current table rather than treating it as a passive snapshot.
        _pendingAddedTables.addAll(currentBrokerAssignments.keySet());
        _initialized = true;
      }
      _observedBrokerAssignments = currentBrokerAssignments;
      pruneDeletedTables();
      propagatePendingChangesForLedTables();
    } catch (Exception e) {
      // A listener or leadership callback failure must not break Helix callback processing. Pending changes remain
      // available for the next BrokerResource notification or leadership acquisition.
      LOGGER.error("Failed to reconcile BrokerResource assignments", e);
    }
  }

  private void pruneDeletedTables() {
    Set<String> existingTables = _tableNamesSupplier.get();
    _pendingAddedTables.retainAll(existingTables);
    _pendingRemovedTables.retainAll(existingTables);
  }

  private void recordChanges(Map<String, Map<String, String>> previousBrokerAssignments,
      Map<String, Map<String, String>> currentBrokerAssignments) {
    Set<String> tables = new TreeSet<>(previousBrokerAssignments.keySet());
    tables.addAll(currentBrokerAssignments.keySet());
    for (String table : tables) {
      Map<String, String> previousAssignment = previousBrokerAssignments.get(table);
      Map<String, String> currentAssignment = currentBrokerAssignments.get(table);
      if (Objects.equals(previousAssignment, currentAssignment)) {
        continue;
      }

      if (previousAssignment == null || (currentAssignment != null
          && hasNewOrChangedAssignment(previousAssignment, currentAssignment))) {
        _pendingAddedTables.add(table);
      }
      if (currentAssignment == null || (previousAssignment != null
          && hasNewOrChangedAssignment(currentAssignment, previousAssignment))) {
        _pendingRemovedTables.add(table);
      }
    }
  }

  private void propagatePendingChangesForLedTables() {
    Set<String> pendingTables = new TreeSet<>(_pendingAddedTables);
    pendingTables.addAll(_pendingRemovedTables);
    List<String> tablesAdded = new ArrayList<>();
    List<String> tablesRemoved = new ArrayList<>();
    for (String table : pendingTables) {
      if (!_leadControllerManager.isLeaderForTable(table)) {
        continue;
      }
      if (_pendingAddedTables.contains(table)) {
        tablesAdded.add(table);
      }
      if (_pendingRemovedTables.contains(table)) {
        tablesRemoved.add(table);
      }
    }

    if (tablesAdded.isEmpty() && tablesRemoved.isEmpty()) {
      return;
    }
    _queryWorkloadManager.onBrokerResourceChanged(tablesAdded, tablesRemoved);
    _pendingAddedTables.removeAll(tablesAdded);
    _pendingRemovedTables.removeAll(tablesRemoved);
  }

  private static boolean isBrokerResourceChange(NotificationContext context) {
    if (context.getIsChildChange()) {
      // A child change is reported on the IdealStates parent path, so a targeted read is needed to distinguish a
      // BrokerResource add/delete from other resource churn.
      return true;
    }
    String pathChanged = context.getPathChanged();
    return pathChanged == null || pathChanged.equals(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        || pathChanged.endsWith("/" + CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
  }

  private static boolean hasNewOrChangedAssignment(Map<String, String> oldAssignment,
      Map<String, String> newAssignment) {
    for (Map.Entry<String, String> entry : newAssignment.entrySet()) {
      if (!Objects.equals(oldAssignment.get(entry.getKey()), entry.getValue())) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, Map<String, String>> getBrokerAssignments(IdealState brokerResource) {
    if (brokerResource == null) {
      return Map.of();
    }
    Map<String, Map<String, String>> brokerAssignments = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : brokerResource.getRecord().getMapFields().entrySet()) {
      brokerAssignments.put(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    return brokerAssignments;
  }
}
