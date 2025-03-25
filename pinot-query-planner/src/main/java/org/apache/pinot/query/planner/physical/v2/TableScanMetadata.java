package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan;


/**
 * Additional metadata for the {@link PhysicalTableScan}.
 */
public class TableScanMetadata {
  private final Set<String> _scannedTables;
  private final Map<Integer, Map<String, List<String>>> _workedIdToSegmentsMap;
  private final Map<String, String> _tableOptions;
  private final Map<String, Set<String>> _unavailableSegmentsMap;
  @Nullable
  private final TimeBoundaryInfo _timeBoundaryInfo;

  public TableScanMetadata(Set<String> scannedTables, Map<Integer, Map<String, List<String>>> workedIdToSegmentsMap,
      Map<String, String> tableOptions, Map<String, Set<String>> unavailableSegmentsMap,
      @Nullable TimeBoundaryInfo timeBoundaryInfo) {
    _scannedTables = scannedTables;
    _workedIdToSegmentsMap = workedIdToSegmentsMap;
    _tableOptions = tableOptions;
    _unavailableSegmentsMap = unavailableSegmentsMap;
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  public Set<String> getScannedTables() {
    return _scannedTables;
  }

  public Map<Integer, Map<String, List<String>>> getWorkedIdToSegmentsMap() {
    return _workedIdToSegmentsMap;
  }

  public Map<String, String> getTableOptions() {
    return _tableOptions;
  }

  public Map<String, Set<String>> getUnavailableSegmentsMap() {
    return _unavailableSegmentsMap;
  }

  @Nullable
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }
}
