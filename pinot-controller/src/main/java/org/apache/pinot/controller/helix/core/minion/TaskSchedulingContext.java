package org.apache.pinot.controller.helix.core.minion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TaskSchedulingContext {
  private Map<String, Set<String>> _tableToTaskNamesMap;
  private String _triggeredBy;
  private String _minionInstanceTag;
  private boolean _isLeader;

  public TaskSchedulingContext() {
    _tableToTaskNamesMap = new HashMap<>();
  }

  public TaskSchedulingContext(List<String> tableNames) {
    _tableToTaskNamesMap = new HashMap<>(tableNames.size());
    tableNames.forEach(tableName -> _tableToTaskNamesMap.put(tableName, null));
  }

  public TaskSchedulingContext(String tableName, String taskName) {
    _tableToTaskNamesMap = new HashMap<>(1);
    _tableToTaskNamesMap.put(tableName, Set.of(taskName));
  }

  public Map<String, Set<String>> getTableToTaskNamesMap() {
    return _tableToTaskNamesMap;
  }

  public TaskSchedulingContext setTableToTaskNamesMap(Map<String, Set<String>> tableToTaskNamesMap) {
    _tableToTaskNamesMap = tableToTaskNamesMap;
    return this;
  }

  public String getTriggeredBy() {
    return _triggeredBy;
  }

  public TaskSchedulingContext setTriggeredBy(String triggeredBy) {
    _triggeredBy = triggeredBy;
    return this;
  }

  public String getMinionInstanceTag() {
    return _minionInstanceTag;
  }

  public TaskSchedulingContext setMinionInstanceTag(String minionInstanceTag) {
    _minionInstanceTag = minionInstanceTag;
    return this;
  }

  public boolean isLeader() {
    return _isLeader;
  }

  public TaskSchedulingContext setLeader(boolean leader) {
    _isLeader = leader;
    return this;
  }
}
