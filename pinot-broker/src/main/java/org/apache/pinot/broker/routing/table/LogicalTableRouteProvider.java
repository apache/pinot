package org.apache.pinot.broker.routing.table;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.broker.requesthandler.LogicalQueryRouteInfo;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.query.planner.physical.table.PhysicalTable;
import org.apache.pinot.query.planner.physical.table.PhysicalTableRoute;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogicalTableRouteProvider implements TableRouteProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableRouteProvider.class);

  private final List<PhysicalTable> _offlineTables;
  private final List<PhysicalTable> _realtimeTables;
  private final List<String> _unavailableSegments = new ArrayList<>();
  private int _numPrunedSegments = 0;

  public static LogicalTableRouteProvider create(LogicalTable logicalTable, TableCache tableCache, RoutingManager routingManager) {
    List<PhysicalTable> offlineTables = new ArrayList<>();
    List<PhysicalTable> realtimeTables = new ArrayList<>();
    for (String physicalTableName : logicalTable.getPhysicalTableNames()) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(physicalTableName);
      Preconditions.checkNotNull(tableType);
      TableConfig tableConfig = tableCache.getTableConfig(physicalTableName);
      if (routingManager.routingExists(physicalTableName)) {
        if (tableType == TableType.OFFLINE) {
          offlineTables.add(new PhysicalTable(physicalTableName, physicalTableName, tableType, tableConfig,
              routingManager.isTableDisabled(physicalTableName)));
        } else {
          realtimeTables.add(new PhysicalTable(physicalTableName, physicalTableName, tableType, tableConfig,
              routingManager.isTableDisabled(physicalTableName)));
        }
      }
    }

    return new LogicalTableRouteProvider(offlineTables, realtimeTables);
  }

  private LogicalTableRouteProvider(List<PhysicalTable> offlineTables, List<PhysicalTable> realtimeTables) {
    _offlineTables = offlineTables;
    _realtimeTables = realtimeTables;
  }

  @Nullable
  @Override
  public TableConfig getOfflineTableConfig() {
    return !_offlineTables.isEmpty() ? _offlineTables.get(0).getTableConfig() : null;
  }

  @Nullable
  @Override
  public TableConfig getRealtimeTableConfig() {
    return !_realtimeTables.isEmpty() ? _realtimeTables.get(0).getTableConfig() : null;
  }

  @Override
  public boolean isExists() {
    return !_offlineTables.isEmpty() || !_realtimeTables.isEmpty();
  }

  @Override
  public boolean isHybrid() {
    return !_offlineTables.isEmpty() && !_realtimeTables.isEmpty();
  }

  @Override
  public boolean isOffline() {
    return !_offlineTables.isEmpty() && _realtimeTables.isEmpty();
  }

  @Override
  public boolean isRealtime() {
    return _offlineTables.isEmpty() && !_realtimeTables.isEmpty();
  }

  @Override
  public boolean hasOffline() {
    return !_offlineTables.isEmpty();
  }

  @Override
  public boolean hasRealtime() {
    return !_realtimeTables.isEmpty();
  }

  @Nullable
  @Override
  public String getOfflineTableName() {
    return !_offlineTables.isEmpty() ? _offlineTables.get(0).getTableName() : null;
  }

  @Nullable
  @Override
  public String getRealtimeTableName() {
    return !_realtimeTables.isEmpty() ? _realtimeTables.get(0).getTableName() : null;
  }

  @Override
  public boolean isRouteExists() {
    return isExists();
  }

  @Override
  public boolean isDisabled() {
    return false;
  }

  @Nullable
  @Override
  public List<String> getDisabledTableNames() {
    return List.of();
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return null;
  }

  @Override
  public TableRouteInfo calculateRoutes(RoutingManager routingManager, BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, long requestId) {
    List<PhysicalTableRoute> offlineTableRoutes = new ArrayList<>();
    for (PhysicalTable physicalTable : _offlineTables) {
      PhysicalTableRoute route =
          PhysicalTableRoute.from(physicalTable.getTableName(), routingManager, offlineBrokerRequest, requestId);
      if (route != null) {
        offlineTableRoutes.add(route);
        _numPrunedSegments += route.getNumPrunedSegments();
        _unavailableSegments.addAll(route.getUnavailableSegments());
      }
    }

    List<PhysicalTableRoute> realtimeTableRoutes = new ArrayList<>();
    for (PhysicalTable physicalTable : _realtimeTables) {
      PhysicalTableRoute route =
          PhysicalTableRoute.from(physicalTable.getTableName(), routingManager, realtimeBrokerRequest, requestId);
      if (route != null) {
        realtimeTableRoutes.add(route);
        _numPrunedSegments += route.getNumPrunedSegments();
        _unavailableSegments.addAll(route.getUnavailableSegments());
      }
    }

    TimeBoundaryInfo timeBoundaryInfo = null;
    if (!offlineTableRoutes.isEmpty() && !realtimeTableRoutes.isEmpty()) {
      timeBoundaryInfo = routingManager.getTimeBoundaryInfo(_offlineTables.get(0).getTableName());
      if (timeBoundaryInfo == null) {
        LOGGER.debug("No time boundary info found for hybrid table: ");
        offlineTableRoutes = List.of();
      }
    }

    return new LogicalQueryRouteInfo(offlineBrokerRequest, realtimeBrokerRequest, offlineTableRoutes,
        realtimeTableRoutes);
  }

  @Override
  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  @Override
  public int getNumPrunedSegmentsTotal() {
    return _numPrunedSegments;
  }
}
