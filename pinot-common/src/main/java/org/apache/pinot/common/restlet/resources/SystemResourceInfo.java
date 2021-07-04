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
package org.apache.pinot.common.restlet.resources;

import com.google.common.collect.ImmutableMap;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;


/**
 * Class to represent system resources (CPU, Memory, etc) for an instance.
 */
@SuppressWarnings("unused")
public class SystemResourceInfo {
  private static final int MEGA_BYTES = 1024 * 1024;

  private final String NUM_CORES_KEY = "numCores";
  private final String TOTAL_MEMORY_MB_KEY = "totalMemoryMB";
  private final String MAX_HEAP_SIZE_MB_KEY = "maxHeapSizeMB";

  private final int _numCores;
  private final long _totalMemoryMB;
  private final long _maxHeapSizeMB;

  /**
   * Constructor that initializes the values from reading system properties.
   */
  public SystemResourceInfo() {
    Runtime runtime = Runtime.getRuntime();
    _numCores = runtime.availableProcessors();
    OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();

    // Not all platforms may implement this, and so we may not have access to some of the api's.
    if (osMXBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsMXBean = (com.sun.management.OperatingSystemMXBean) osMXBean;
      _totalMemoryMB = sunOsMXBean.getTotalPhysicalMemorySize() / MEGA_BYTES;
    } else {
      _totalMemoryMB = runtime.totalMemory() / MEGA_BYTES;
    }

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
    _maxHeapSizeMB = heapMemoryUsage.getMax() / MEGA_BYTES;
  }

  /**
   * Constructor of class from map.
   * @param map Map containing values for member variables.
   */
  public SystemResourceInfo(Map<String, String> map) {
    _numCores = Integer.parseInt(map.get(NUM_CORES_KEY));
    _totalMemoryMB = Long.parseLong(map.get(TOTAL_MEMORY_MB_KEY));
    _maxHeapSizeMB = Long.parseLong(map.get(MAX_HEAP_SIZE_MB_KEY));
  }

  public int getNumCores() {
    return _numCores;
  }

  public long getTotalMemoryMB() {
    return _totalMemoryMB;
  }

  public long getMaxHeapSizeMB() {
    return _maxHeapSizeMB;
  }

  /**
   * Returns a map containing names of fields along with their String values.
   *
   * @return Map of field names to values
   */
  public Map<String, String> toMap() {
    Map<String, String> map = new HashMap<>();
    map.put(NUM_CORES_KEY, Integer.toString(_numCores));
    map.put(TOTAL_MEMORY_MB_KEY, Long.toString(_totalMemoryMB));
    map.put(MAX_HEAP_SIZE_MB_KEY, Long.toString(_maxHeapSizeMB));
    return ImmutableMap.copyOf(map);
  }
}
