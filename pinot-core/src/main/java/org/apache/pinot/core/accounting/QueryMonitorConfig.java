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
package org.apache.pinot.core.accounting;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;


public class QueryMonitorConfig {
  private final long _maxHeapSize;

  // don't kill a query if its memory footprint is below some ratio of _maxHeapSize
  private final long _minMemoryFootprintForKill;

  // kill all queries if heap usage exceeds this
  private final long _panicLevel;

  // kill the most expensive query if heap usage exceeds this
  private final long _criticalLevel;

  // start to sample more frequently if heap usage exceeds this
  private final long _alarmingLevel;

  // normal sleep time
  private final int _normalSleepTime;

  // alarming sleep time denominator, should be > 1 to sample more frequent at alarming level
  private final int _alarmingSleepTimeDenominator;

  // alarming sleep time
  private final int _alarmingSleepTime;

  // the framework would not commit to kill any query if this is disabled
  private final boolean _oomKillQueryEnabled;

  // if we want to publish the heap usage
  private final boolean _publishHeapUsageMetric;

  // if we want kill query based on CPU time
  private final boolean _cpuTimeBasedKillingEnabled;

  // CPU time based killing threshold
  private final long _cpuTimeBasedKillingThresholdNs;

  private final boolean _queryKilledMetricEnabled;

  // how long to pause query threads (ms) before proceeding with kill; non-positive means disabled
  private final long _oomPreQueryKillPauseDurationMs;

  // whether to also apply the OOM pause before killing at panic level
  private final boolean _oomPanicPreQueryKillPauseEnabled;

  private final int _workloadSleepTimeMs;

  private final boolean _workloadCostEnforcementEnabled;

  public QueryMonitorConfig(PinotConfiguration config, long maxHeapSize) {
    _maxHeapSize = maxHeapSize;

    _minMemoryFootprintForKill =
        (long) (maxHeapSize * config.getProperty(Accounting.Keys.MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO,
            Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO));

    _panicLevel = (long) (maxHeapSize * config.getProperty(Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO,
        Accounting.DEFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO));

    // kill the most expensive query if heap usage exceeds this
    _criticalLevel = (long) (maxHeapSize * config.getProperty(Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO,
        Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO));

    _alarmingLevel = (long) (maxHeapSize * config.getProperty(Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO,
        Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO));

    _normalSleepTime = config.getProperty(Accounting.Keys.SLEEP_TIME_MS, Accounting.DEFAULT_SLEEP_TIME_MS);

    _alarmingSleepTimeDenominator =
        config.getProperty(Accounting.Keys.SLEEP_TIME_DENOMINATOR, Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);

    _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeDenominator;

    _oomKillQueryEnabled = config.getProperty(Accounting.Keys.OOM_PROTECTION_KILLING_QUERY,
        Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY);

    _publishHeapUsageMetric =
        config.getProperty(Accounting.Keys.PUBLISHING_JVM_USAGE, Accounting.DEFAULT_PUBLISHING_JVM_USAGE);

    _cpuTimeBasedKillingEnabled = config.getProperty(Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED,
        Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED);

    _cpuTimeBasedKillingThresholdNs = config.getProperty(Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS,
        Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS) * 1000_000L;

    _queryKilledMetricEnabled =
        config.getProperty(Accounting.Keys.QUERY_KILLED_METRIC_ENABLED, Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED);

    _oomPreQueryKillPauseDurationMs = config.getProperty(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS,
        Accounting.DEFAULT_OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS);

    _oomPanicPreQueryKillPauseEnabled = config.getProperty(Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE,
        Accounting.DEFAULT_OOM_PANIC_PRE_QUERY_KILL_PAUSE_ENABLED);

    _workloadSleepTimeMs =
        config.getProperty(Accounting.Keys.WORKLOAD_SLEEP_TIME_MS, Accounting.DEFAULT_WORKLOAD_SLEEP_TIME_MS);

    _workloadCostEnforcementEnabled = config.getProperty(Accounting.Keys.WORKLOAD_ENABLE_COST_ENFORCEMENT,
        Accounting.DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT);
  }

  QueryMonitorConfig(QueryMonitorConfig oldConfig, Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    _maxHeapSize = oldConfig._maxHeapSize;

    if (changedConfigs.contains(Accounting.Keys.MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO)) {
      String str = clusterConfigs.get(Accounting.Keys.MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO);
      double value = str != null ? Double.parseDouble(str) : Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO;
      _minMemoryFootprintForKill = (long) (_maxHeapSize * value);
    } else {
      _minMemoryFootprintForKill = oldConfig._minMemoryFootprintForKill;
    }

    if (changedConfigs.contains(Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO)) {
      String str = clusterConfigs.get(Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO);
      double value = str != null ? Double.parseDouble(str) : Accounting.DEFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO;
      _panicLevel = (long) (_maxHeapSize * value);
    } else {
      _panicLevel = oldConfig._panicLevel;
    }

    if (changedConfigs.contains(Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO)) {
      String str = clusterConfigs.get(Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO);
      double value = str != null ? Double.parseDouble(str) : Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO;
      _criticalLevel = (long) (_maxHeapSize * value);
    } else {
      _criticalLevel = oldConfig._criticalLevel;
    }

    if (changedConfigs.contains(Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO)) {
      String str = clusterConfigs.get(Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO);
      double value = str != null ? Double.parseDouble(str) : Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO;
      _alarmingLevel = (long) (_maxHeapSize * value);
    } else {
      _alarmingLevel = oldConfig._alarmingLevel;
    }

    if (changedConfigs.contains(Accounting.Keys.SLEEP_TIME_MS)) {
      String str = clusterConfigs.get(Accounting.Keys.SLEEP_TIME_MS);
      _normalSleepTime = str != null ? Integer.parseInt(str) : Accounting.DEFAULT_SLEEP_TIME_MS;
    } else {
      _normalSleepTime = oldConfig._normalSleepTime;
    }

    if (changedConfigs.contains(Accounting.Keys.SLEEP_TIME_DENOMINATOR)) {
      String str = clusterConfigs.get(Accounting.Keys.SLEEP_TIME_DENOMINATOR);
      _alarmingSleepTimeDenominator = str != null ? Integer.parseInt(str) : Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR;
    } else {
      _alarmingSleepTimeDenominator = oldConfig._alarmingSleepTimeDenominator;
    }

    _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeDenominator;

    if (changedConfigs.contains(Accounting.Keys.OOM_PROTECTION_KILLING_QUERY)) {
      String str = clusterConfigs.get(Accounting.Keys.OOM_PROTECTION_KILLING_QUERY);
      _oomKillQueryEnabled =
          str != null ? Boolean.parseBoolean(str) : Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY;
    } else {
      _oomKillQueryEnabled = oldConfig._oomKillQueryEnabled;
    }

    if (changedConfigs.contains(Accounting.Keys.PUBLISHING_JVM_USAGE)) {
      String str = clusterConfigs.get(Accounting.Keys.PUBLISHING_JVM_USAGE);
      _publishHeapUsageMetric = str != null ? Boolean.parseBoolean(str) : Accounting.DEFAULT_PUBLISHING_JVM_USAGE;
    } else {
      _publishHeapUsageMetric = oldConfig._publishHeapUsageMetric;
    }

    if (changedConfigs.contains(Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED)) {
      String str = clusterConfigs.get(Accounting.Keys.CPU_TIME_BASED_KILLING_ENABLED);
      _cpuTimeBasedKillingEnabled =
          str != null ? Boolean.parseBoolean(str) : Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED;
    } else {
      _cpuTimeBasedKillingEnabled = oldConfig._cpuTimeBasedKillingEnabled;
    }

    if (changedConfigs.contains(Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS)) {
      String str = clusterConfigs.get(Accounting.Keys.CPU_TIME_BASED_KILLING_THRESHOLD_MS);
      long valueMs = str != null ? Long.parseLong(str) : Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS;
      _cpuTimeBasedKillingThresholdNs = valueMs * 1000_000L;
    } else {
      _cpuTimeBasedKillingThresholdNs = oldConfig._cpuTimeBasedKillingThresholdNs;
    }

    if (changedConfigs.contains(Accounting.Keys.QUERY_KILLED_METRIC_ENABLED)) {
      String str = clusterConfigs.get(Accounting.Keys.QUERY_KILLED_METRIC_ENABLED);
      _queryKilledMetricEnabled =
          str != null ? Boolean.parseBoolean(str) : Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED;
    } else {
      _queryKilledMetricEnabled = oldConfig._queryKilledMetricEnabled;
    }

    if (changedConfigs.contains(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS)) {
      String str = clusterConfigs.get(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS);
      _oomPreQueryKillPauseDurationMs =
          str != null ? Long.parseLong(str) : Accounting.DEFAULT_OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS;
    } else {
      _oomPreQueryKillPauseDurationMs = oldConfig._oomPreQueryKillPauseDurationMs;
    }

    if (changedConfigs.contains(Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE)) {
      String str = clusterConfigs.get(Accounting.Keys.OOM_PANIC_ALLOW_PRE_QUERY_KILL_PAUSE);
      _oomPanicPreQueryKillPauseEnabled =
          str != null ? Boolean.parseBoolean(str) : Accounting.DEFAULT_OOM_PANIC_PRE_QUERY_KILL_PAUSE_ENABLED;
    } else {
      _oomPanicPreQueryKillPauseEnabled = oldConfig._oomPanicPreQueryKillPauseEnabled;
    }

    if (changedConfigs.contains(Accounting.Keys.WORKLOAD_SLEEP_TIME_MS)) {
      String str = clusterConfigs.get(Accounting.Keys.WORKLOAD_SLEEP_TIME_MS);
      _workloadSleepTimeMs = str != null ? Integer.parseInt(str) : Accounting.DEFAULT_WORKLOAD_SLEEP_TIME_MS;
    } else {
      _workloadSleepTimeMs = oldConfig._workloadSleepTimeMs;
    }

    if (changedConfigs.contains(Accounting.Keys.WORKLOAD_ENABLE_COST_ENFORCEMENT)) {
      String str = clusterConfigs.get(Accounting.Keys.WORKLOAD_ENABLE_COST_ENFORCEMENT);
      _workloadCostEnforcementEnabled =
          str != null ? Boolean.parseBoolean(str) : Accounting.DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT;
    } else {
      _workloadCostEnforcementEnabled = oldConfig._workloadCostEnforcementEnabled;
    }
  }

  public long getMaxHeapSize() {
    return _maxHeapSize;
  }

  public long getMinMemoryFootprintForKill() {
    return _minMemoryFootprintForKill;
  }

  public long getPanicLevel() {
    return _panicLevel;
  }

  public long getCriticalLevel() {
    return _criticalLevel;
  }

  public long getAlarmingLevel() {
    return _alarmingLevel;
  }

  public int getNormalSleepTime() {
    return _normalSleepTime;
  }

  public int getAlarmingSleepTime() {
    return _alarmingSleepTime;
  }

  public boolean isOomKillQueryEnabled() {
    return _oomKillQueryEnabled;
  }

  public boolean isPublishHeapUsageMetric() {
    return _publishHeapUsageMetric;
  }

  public boolean isCpuTimeBasedKillingEnabled() {
    return _cpuTimeBasedKillingEnabled;
  }

  public long getCpuTimeBasedKillingThresholdNs() {
    return _cpuTimeBasedKillingThresholdNs;
  }

  public boolean isQueryKilledMetricEnabled() {
    return _queryKilledMetricEnabled;
  }

  public boolean isOomPreQueryKillPauseEnabled() {
    return _oomPreQueryKillPauseDurationMs > 0;
  }

  public long getOomPreQueryKillPauseDurationMs() {
    return _oomPreQueryKillPauseDurationMs;
  }

  public boolean isOomPanicPreQueryKillPauseEnabled() {
    return _oomPanicPreQueryKillPauseEnabled;
  }

  public int getWorkloadSleepTimeMs() {
    return _workloadSleepTimeMs;
  }

  public boolean isWorkloadCostEnforcementEnabled() {
    return _workloadCostEnforcementEnabled;
  }
}
