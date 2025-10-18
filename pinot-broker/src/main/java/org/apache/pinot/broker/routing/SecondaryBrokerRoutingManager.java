package org.apache.pinot.broker.routing;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;


public class SecondaryBrokerRoutingManager extends BrokerRoutingManager {

  public SecondaryBrokerRoutingManager(BrokerMetrics brokerMetrics,
      ServerRoutingStatsManager serverRoutingStatsManager,
      PinotConfiguration pinotConfig) {
    super(brokerMetrics, serverRoutingStatsManager, pinotConfig);
  }

  @Override
  public void init(HelixManager helixManager) {
    super.init(helixManager);
  }

  public void initAllTablesFromZk() {
    String externalViewPath =
      _externalViewPathPrefix.substring(0, _externalViewPathPrefix.length() - 1);

    // Tables currently visible in ZK
    Set<String> currentTables = _zkDataAccessor.getChildNames(externalViewPath, 0).stream()
      .filter(this::isPinotTableName)
      .collect(Collectors.toSet());

    // Tables we currently have routing for
    Set<String> knownTables = new HashSet<>(_routingEntryMap.keySet());

    // Diff: what to add / what to remove
    Set<String> toAdd = new HashSet<>(currentTables);
    toAdd.removeAll(knownTables);

    Set<String> toRemove = new HashSet<>(knownTables);
    toRemove.removeAll(currentTables);

    toAdd.forEach(this::addRouting);
    toRemove.forEach(this::dropRouting);
  }

  private boolean isPinotTableName(String table) {
    return table.endsWith("_OFFLINE") || table.endsWith("_REALTIME");
  }

  private void addRouting(String table) {
    System.out.println("<<<<< >>>>> BUILDING ROUTING IN SecondaryBrokerRoutingManager FOR TABLE: "
      + table + " in " + _parentClusterName);
    if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, table)) {
      buildRoutingForLogicalTable(table);
    } else {
      buildRouting(table);
    }
  }

  private void dropRouting(String table) {
    System.out.println("<<<<< >>>>> REMOVING ROUTING IN SecondaryBrokerRoutingManager FOR TABLE: "
      + table + " in " + _parentClusterName);
    if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, table)) {
      removeRoutingForLogicalTable(table);
    } else {
      removeRouting(table);
    }
  }

  @Override
  protected void processSegmentAssignmentChangeInternal() {
    System.out.println("<<<<< >>>>> Processing segment assignment change in SecondaryBrokerRoutingManager for cluster:"
      + _parentClusterName);
    super.processSegmentAssignmentChangeInternal();
    initAllTablesFromZk();
  }

//  /**
//   * @deprecated Use {@link #initAllTablesFromZk()} instead.
//   */
//  protected void deprecatedProcessBrokerResourceConfigChange() {
//    String brokerResourcePath = _externalViewPathPrefix + "brokerResource";
//    Stat stat = new Stat();
//    ZNRecord znRecord = _zkDataAccessor.get(brokerResourcePath, stat, AccessOption.PERSISTENT);
//
//    if (znRecord == null || znRecord.getMapFields() == null) {
//      return;
//    }
//    Map<String, Map<String, String>> zkTableState = znRecord.getMapFields();
//
//    for (Map.Entry<String, Map<String, String>> entry : zkTableState.entrySet()) {
//      String physicalOrLogicalTable = entry.getKey();
//      Map<String, String> stateMap = entry.getValue();
//      String currentTableState = _tableState.getOrDefault(physicalOrLogicalTable, "OFFLINE");
//      String newTableState = stateMap.values().stream().anyMatch("ONLINE"::equals) ? "ONLINE" : "OFFLINE";
//
//      if ("ONLINE".equals(newTableState) && !"ONLINE".equals(currentTableState)) {
//        // If the table state changes to ONLINE, we need to update the routing table
//        System.out.println("BUILDING ROUTING FOR TABLE: " + physicalOrLogicalTable);
//        if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, physicalOrLogicalTable)) {
//          buildRoutingForLogicalTable(physicalOrLogicalTable);
//        } else {
//          buildRouting(physicalOrLogicalTable);
//        }
//      } else if ("OFFLINE".equals(newTableState) && "ONLINE".equals(currentTableState)) {
//        System.out.println("REMOVING ROUTING FOR TABLE: " + physicalOrLogicalTable);
//        // If the table state changes to OFFLINE, we can remove it from the routing table
//        if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, physicalOrLogicalTable)) {
//          removeRoutingForLogicalTable(physicalOrLogicalTable);
//        } else {
//          removeRouting(physicalOrLogicalTable);
//        }
//      }
//      // Update the current state in the local map
//      _tableState.put(physicalOrLogicalTable, newTableState);
//    }
//  }
}
