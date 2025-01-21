package org.apache.pinot.controller.helix.core.minion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Wrapper class to manage all the inputs passed to schedule a task on minion
 * _tableToTaskNamesMap is a map of table name and its consecutive task types for which task needs to be scheduled
 * <p>
 * Few special cases to note :
 * <li>If the value for a table name entry is null or empty set then
 *    it will end up scheduling all the configured tasks on that table.
 * <li>If the _tableToTaskNamesMap is empty then it will end up scheduling
 *    all the configured tasks for all the tables.
 */
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
