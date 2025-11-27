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
import org.apache.pinot.spi.utils.CommonConstants;


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

  private final int _workloadSleepTimeMs;

  private final boolean _workloadCostEnforcementEnabled;

  public QueryMonitorConfig(PinotConfiguration config, long maxHeapSize) {
    _maxHeapSize = maxHeapSize;

    _minMemoryFootprintForKill = (long) (maxHeapSize * config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO,
        CommonConstants.Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO));

    _panicLevel =
        (long) (maxHeapSize * config.getProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO));

    // kill the most expensive query if heap usage exceeds this
    _criticalLevel =
        (long) (maxHeapSize * config.getProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO));

    _alarmingLevel =
        (long) (maxHeapSize * config.getProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO));

    _normalSleepTime = config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS,
        CommonConstants.Accounting.DEFAULT_SLEEP_TIME_MS);

    _alarmingSleepTimeDenominator = config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR,
        CommonConstants.Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);

    _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeDenominator;

    _oomKillQueryEnabled = config.getProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY,
        CommonConstants.Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY);

    _publishHeapUsageMetric = config.getProperty(CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE,
        CommonConstants.Accounting.DEFAULT_PUBLISHING_JVM_USAGE);

    _cpuTimeBasedKillingEnabled =
        config.getProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED,
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED);

    _cpuTimeBasedKillingThresholdNs =
        config.getProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS,
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS) * 1000_000L;

    _queryKilledMetricEnabled = config.getProperty(CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED,
        CommonConstants.Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED);

    _workloadSleepTimeMs = config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_SLEEP_TIME_MS);

    _workloadCostEnforcementEnabled =
        config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT,
            CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT);
  }

  QueryMonitorConfig(QueryMonitorConfig oldConfig, Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    _maxHeapSize = oldConfig._maxHeapSize;

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO)) {
        _minMemoryFootprintForKill =
            (long) (_maxHeapSize * CommonConstants.Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO);
      } else {
        _minMemoryFootprintForKill = (long) (_maxHeapSize * Double.parseDouble(
            clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO)));
      }
    } else {
      _minMemoryFootprintForKill = oldConfig._minMemoryFootprintForKill;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO)) {
        _panicLevel = (long) (_maxHeapSize * CommonConstants.Accounting.DEFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO);
      } else {
        _panicLevel = (long) (_maxHeapSize * Double.parseDouble(
            clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO)));
      }
    } else {
      _panicLevel = oldConfig._panicLevel;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO)) {
        _criticalLevel = (long) (_maxHeapSize * CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO);
      } else {
        _criticalLevel = (long) (_maxHeapSize * Double.parseDouble(
            clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO)));
      }
    } else {
      _criticalLevel = oldConfig._criticalLevel;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO)) {
        _alarmingLevel = (long) (_maxHeapSize * CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO);
      } else {
        _alarmingLevel = (long) (_maxHeapSize * Double.parseDouble(
            clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO)));
      }
    } else {
      _alarmingLevel = oldConfig._alarmingLevel;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS)) {
        _normalSleepTime = CommonConstants.Accounting.DEFAULT_SLEEP_TIME_MS;
      } else {
        _normalSleepTime = Integer.parseInt(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS));
      }
    } else {
      _normalSleepTime = oldConfig._normalSleepTime;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR)) {
        _alarmingSleepTimeDenominator = CommonConstants.Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR;
      } else {
        _alarmingSleepTimeDenominator =
            Integer.parseInt(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR));
      }
    } else {
      _alarmingSleepTimeDenominator = oldConfig._alarmingSleepTimeDenominator;
    }

    _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeDenominator;

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY)) {
        _oomKillQueryEnabled = CommonConstants.Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY;
      } else {
        _oomKillQueryEnabled =
            Boolean.parseBoolean(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY));
      }
    } else {
      _oomKillQueryEnabled = oldConfig._oomKillQueryEnabled;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE)) {
        _publishHeapUsageMetric = CommonConstants.Accounting.DEFAULT_PUBLISHING_JVM_USAGE;
      } else {
        _publishHeapUsageMetric =
            Boolean.parseBoolean(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE));
      }
    } else {
      _publishHeapUsageMetric = oldConfig._publishHeapUsageMetric;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED)) {
        _cpuTimeBasedKillingEnabled = CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED;
      } else {
        _cpuTimeBasedKillingEnabled = Boolean.parseBoolean(
            clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED));
      }
    } else {
      _cpuTimeBasedKillingEnabled = oldConfig._cpuTimeBasedKillingEnabled;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS)) {
        _cpuTimeBasedKillingThresholdNs =
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS * 1000_000L;
      } else {
        _cpuTimeBasedKillingThresholdNs =
            Long.parseLong(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS))
                * 1000_000L;
      }
    } else {
      _cpuTimeBasedKillingThresholdNs = oldConfig._cpuTimeBasedKillingThresholdNs;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED)) {
        _queryKilledMetricEnabled = CommonConstants.Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED;
      } else {
        _queryKilledMetricEnabled =
            Boolean.parseBoolean(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED));
      }
    } else {
      _queryKilledMetricEnabled = oldConfig._queryKilledMetricEnabled;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS)) {
        _workloadSleepTimeMs = CommonConstants.Accounting.DEFAULT_WORKLOAD_SLEEP_TIME_MS;
      } else {
        _workloadSleepTimeMs =
            Integer.parseInt(clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS));
      }
    } else {
      _workloadSleepTimeMs = oldConfig._workloadSleepTimeMs;
    }

    if (changedConfigs.contains(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT)) {
      if (clusterConfigs == null || !clusterConfigs.containsKey(
          CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT)) {
        _workloadCostEnforcementEnabled = CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT;
      } else {
        _workloadCostEnforcementEnabled = Boolean.parseBoolean(
            clusterConfigs.get(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT));
      }
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

  public int getWorkloadSleepTimeMs() {
    return _workloadSleepTimeMs;
  }

  public boolean isWorkloadCostEnforcementEnabled() {
    return _workloadCostEnforcementEnabled;
  }
}
