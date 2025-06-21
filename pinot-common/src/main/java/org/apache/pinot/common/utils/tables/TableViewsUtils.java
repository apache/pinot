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
package org.apache.pinot.common.utils.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class TableViewsUtils {
  public static final String IDEALSTATE = "idealstate";
  public static final String EXTERNALVIEW = "externalview";

  private TableViewsUtils() {
  }

  public static class TableView {
    @JsonProperty("OFFLINE")
    public Map<String, Map<String, String>> _offline;
    @JsonProperty("REALTIME")
    public Map<String, Map<String, String>> _realtime;
  }

  public static TableViewsUtils.TableView getSegmentsView(TableViewsUtils.TableView tableView,
      List<String> segmentNames) {
    TableViewsUtils.TableView tableViewResult = new TableViewsUtils.TableView();
    if (tableView._offline != null) {
      tableViewResult._offline = getTableTypeSegmentsView(tableView._offline, segmentNames);
    }
    if (tableView._realtime != null) {
      tableViewResult._realtime = getTableTypeSegmentsView(tableView._realtime, segmentNames);
    }
    return tableViewResult;
  }

  public static List<SegmentStatusInfo> getSegmentStatuses(Map<String, Map<String, String>> externalViewMap,
      Map<String, Map<String, String>> idealStateMap) {
    return getSegmentStatuses(externalViewMap, idealStateMap, null);
  }

  public static List<SegmentStatusInfo> getSegmentStatuses(Map<String, Map<String, String>> externalViewMap,
      Map<String, Map<String, String>> idealStateMap, @Nullable String filterStatus) {
    List<SegmentStatusInfo> segmentStatusInfoList = new ArrayList<>();

    for (Map.Entry<String, Map<String, String>> entry : idealStateMap.entrySet()) {
      String segment = entry.getKey();
      Map<String, String> externalViewEntryValue = externalViewMap.get(segment);
      Map<String, String> idealViewEntryValue = entry.getValue();

      String computedStatus = computeDisplayStatus(externalViewEntryValue, idealViewEntryValue);
      if ((filterStatus == null) || (computedStatus.equals(filterStatus))) {
        segmentStatusInfoList.add(new SegmentStatusInfo(segment, computedStatus));
      }
    }

    return segmentStatusInfoList;
  }

  public static Map<String, String> getSegmentStatusesMap(Map<String, Map<String, String>> externalViewMap,
      Map<String, Map<String, String>> idealStateMap) {
    Map<String, String> segmentStatusInfoList = new HashMap<>();

    for (Map.Entry<String, Map<String, String>> entry : idealStateMap.entrySet()) {
      String segment = entry.getKey();
      Map<String, String> externalViewEntryValue = externalViewMap.get(segment);
      Map<String, String> idealViewEntryValue = entry.getValue();

      String computedStatus = computeDisplayStatus(externalViewEntryValue, idealViewEntryValue);
      segmentStatusInfoList.put(segment, computedStatus);
    }
    return segmentStatusInfoList;
  }

  public static String computeDisplayStatus(Map<String, String> externalView, Map<String, String> idealView) {
    if (externalView == null) {
      return CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING;
    }

    if (isErrorSegment(externalView)) {
      return CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD;
    }

    if (externalView.equals(idealView)) {
      if (isOnlineOrConsumingSegment(externalView) || isOfflineSegment(externalView)) {
        return CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD;
      } else {
        return CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING;
      }
    }

    return CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING;
  }

  public static Map<String, Map<String, String>> getTableTypeSegmentsView(
      Map<String, Map<String, String>> tableTypeView, List<String> segmentNames) {
    Map<String, Map<String, String>> tableTypeViewResult = new HashMap<>();
    for (String segmentName : segmentNames) {
      Map<String, String> segmentView = tableTypeView.get(segmentName);
      if (segmentView != null) {
        tableTypeViewResult.put(segmentName, segmentView);
      }
    }
    return tableTypeViewResult;
  }

  public static Map<String, Map<String, String>> getStateMap(TableViewsUtils.TableView view) {
    if (view != null && view._offline != null && !view._offline.isEmpty()) {
      return view._offline;
    } else if (view != null && view._realtime != null && !view._realtime.isEmpty()) {
      return view._realtime;
    } else {
      return new HashMap<>();
    }
  }

  public static boolean isErrorSegment(Map<String, String> stateMap) {
    return stateMap.values().contains(CommonConstants.Helix.StateModel.SegmentStateModel.ERROR);
  }

  public static boolean isOnlineOrConsumingSegment(Map<String, String> stateMap) {
    return stateMap.values().stream().allMatch(
        state -> state.equals(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING) || state.equals(
            CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
  }

  public static boolean isOfflineSegment(Map<String, String> stateMap) {
    return stateMap.values().contains(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE);
  }

  // we use name "view" to closely match underlying names and to not
  // confuse with table state of enable/disable
  public static TableViewsUtils.TableView getTableState(String tableName, String view, @Nullable TableType tableType,
      HelixAdmin helixAdmin, String helixClusterName)
      throws Exception {
    TableViewsUtils.TableView tableView;
    if (view.equalsIgnoreCase(IDEALSTATE)) {
      tableView = getTableIdealState(tableName, tableType, helixAdmin, helixClusterName);
    } else if (view.equalsIgnoreCase(EXTERNALVIEW)) {
      tableView = getTableExternalView(tableName, tableType, helixAdmin, helixClusterName);
    } else {
      throw new Exception(
          "Bad view name: " + view + ". Expected idealstate or externalview");
    }

    if (tableView._offline == null && tableView._realtime == null) {
      throw new Exception("Table not found");
    }
    return tableView;
  }

  public static TableViewsUtils.TableView getTableIdealState(String tableNameOptType, @Nullable TableType tableType,
      HelixAdmin helixAdmin, String helixClusterName) {
    TableViewsUtils.TableView tableView = new TableViewsUtils.TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView._offline = getIdealState(tableNameOptType, TableType.OFFLINE, helixAdmin, helixClusterName);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView._realtime = getIdealState(tableNameOptType, TableType.REALTIME, helixAdmin, helixClusterName);
    }
    return tableView;
  }

  public static TableViewsUtils.TableView getTableExternalView(@Nonnull String tableNameOptType,
      @Nullable TableType tableType, HelixAdmin helixAdmin, String helixClusterName) {
    TableViewsUtils.TableView tableView = new TableViewsUtils.TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView._offline = getExternalView(tableNameOptType, TableType.OFFLINE, helixAdmin, helixClusterName);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView._realtime = getExternalView(tableNameOptType, TableType.REALTIME, helixAdmin, helixClusterName);
    }
    return tableView;
  }

  public static TableType validateTableType(String tableTypeStr)
      throws Exception {
    if (tableTypeStr == null) {
      return null;
    }
    try {
      return TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      String errStr = "Illegal table type '" + tableTypeStr + "'";
      throw new Exception(errStr);
    }
  }

  @Nullable
  public static Map<String, Map<String, String>> getIdealState(@Nonnull String tableNameOptType,
      @Nullable TableType tableType, HelixAdmin helixAdmin, String helixClusterName) {
    String tableNameWithType = getTableNameWithType(tableNameOptType, tableType);
    IdealState resourceIdealState = helixAdmin.getResourceIdealState(helixClusterName, tableNameWithType);
    return resourceIdealState == null ? null : resourceIdealState.getRecord().getMapFields();
  }

  @Nullable
  public static Map<String, Map<String, String>> getExternalView(@Nonnull String tableNameOptType, TableType tableType,
      HelixAdmin helixAdmin, String helixClusterName) {
    String tableNameWithType = getTableNameWithType(tableNameOptType, tableType);
    ExternalView resourceEV = helixAdmin.getResourceExternalView(helixClusterName, tableNameWithType);
    return resourceEV == null ? null : resourceEV.getRecord().getMapFields();
  }

  public static String getTableNameWithType(@Nonnull String tableNameOptType, @Nullable TableType tableType) {
    if (tableType != null) {
      if (tableType == TableType.OFFLINE) {
        return TableNameBuilder.OFFLINE.tableNameWithType(tableNameOptType);
      } else {
        return TableNameBuilder.REALTIME.tableNameWithType(tableNameOptType);
      }
    } else {
      return tableNameOptType;
    }
  }
}
