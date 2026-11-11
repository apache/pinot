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
package org.apache.pinot.spi.accounting;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This class provides the functionality of measuring the CPU time and allocateBytes (JVM heap) for current thread.
public class ThreadResourceUsageProvider {
  private ThreadResourceUsageProvider() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadResourceUsageProvider.class);

  // used for getting the memory allocation function in hotspot jvm through reflection
  private static final String SUN_THREAD_MXBEAN_CLASS_NAME = "com.sun.management.ThreadMXBean";
  private static final String SUN_IS_THREAD_ALLOCATED_MEMORY_SUPPORTED_NAME = "isThreadAllocatedMemorySupported";
  private static final String SUN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_NAME = "setThreadAllocatedMemoryEnabled";
  private static final String SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_NAME = "getCurrentThreadAllocatedBytes";
  private static final String SUN_GET_THREAD_ALLOCATED_BYTES_NAME = "getThreadAllocatedBytes";

  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean IS_CURRENT_THREAD_CPU_TIME_SUPPORTED = MX_BEAN.isCurrentThreadCpuTimeSupported();
  private static final boolean IS_THREAD_ALLOCATED_MEMORY_SUPPORTED;
  private static final Method SUN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_METHOD;
  private static final Method SUN_GET_THREAD_ALLOCATED_BYTES_METHOD;
  private static final Method SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_METHOD;

  private static boolean _isThreadCpuTimeMeasurementEnabled;
  private static boolean _isThreadMemoryMeasurementEnabled;

  // Initialize the com.sun.management.ThreadMXBean related variables using reflection
  static {
    Class<?> sunThreadMXBeanClass = null;
    try {
      sunThreadMXBeanClass = Class.forName(SUN_THREAD_MXBEAN_CLASS_NAME);
    } catch (Exception e) {
      LOGGER.error("Caught exception while loading: {}, you are probably not using Hotspot jvm",
          SUN_THREAD_MXBEAN_CLASS_NAME, e);
    }

    boolean isThreadAllocatedMemorySupported = false;
    Method setThreadAllocatedMemoryEnabled = null;
    Method getCurrentThreadAllocatedBytes = null;
    Method getThreadAllocatedBytes = null;
    if (sunThreadMXBeanClass != null) {
      try {
        isThreadAllocatedMemorySupported =
            (boolean) sunThreadMXBeanClass.getMethod(SUN_IS_THREAD_ALLOCATED_MEMORY_SUPPORTED_NAME).invoke(MX_BEAN);
      } catch (Exception e) {
        LOGGER.error("Caught exception invoking method: {}", SUN_IS_THREAD_ALLOCATED_MEMORY_SUPPORTED_NAME, e);
      }
      if (isThreadAllocatedMemorySupported) {
        try {
          setThreadAllocatedMemoryEnabled =
              sunThreadMXBeanClass.getMethod(SUN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_NAME, boolean.class);
        } catch (Exception e) {
          LOGGER.error("Caught exception loading method: {}", SUN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_NAME, e);
          isThreadAllocatedMemorySupported = false;
        }
      }
      if (isThreadAllocatedMemorySupported) {
        try {
          getCurrentThreadAllocatedBytes = sunThreadMXBeanClass.getMethod(SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_NAME);
        } catch (Exception e1) {
          LOGGER.info("Failed to load method: {}, loading: {} instead", SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_NAME,
              SUN_GET_THREAD_ALLOCATED_BYTES_NAME);
          try {
            getThreadAllocatedBytes = sunThreadMXBeanClass.getMethod(SUN_GET_THREAD_ALLOCATED_BYTES_NAME, long.class);
          } catch (Exception e2) {
            LOGGER.error("Caught exception loading method: {}", SUN_GET_THREAD_ALLOCATED_BYTES_NAME, e2);
            isThreadAllocatedMemorySupported = false;
          }
        }
      }
    }

    LOGGER.info("Current thread CPU time supported: {}", IS_CURRENT_THREAD_CPU_TIME_SUPPORTED);
    LOGGER.info("Thread allocated memory supported: {}", isThreadAllocatedMemorySupported);
    if (isThreadAllocatedMemorySupported) {
      LOGGER.info("Using: {} to read current thread allocated bytes",
          getCurrentThreadAllocatedBytes != null ? SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_NAME
              : SUN_GET_THREAD_ALLOCATED_BYTES_NAME);
    }
    IS_THREAD_ALLOCATED_MEMORY_SUPPORTED = isThreadAllocatedMemorySupported;
    SUN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_METHOD = setThreadAllocatedMemoryEnabled;
    SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_METHOD = getCurrentThreadAllocatedBytes;
    SUN_GET_THREAD_ALLOCATED_BYTES_METHOD = getThreadAllocatedBytes;
  }

  public static boolean isThreadCpuTimeMeasurementEnabled() {
    return _isThreadCpuTimeMeasurementEnabled;
  }

  public static void setThreadCpuTimeMeasurementEnabled(boolean enable) {
    if (!IS_CURRENT_THREAD_CPU_TIME_SUPPORTED) {
      assert !_isThreadCpuTimeMeasurementEnabled;
      if (enable) {
        LOGGER.error("Not enabling thread CPU time measurement because it is not supported");
      }
      return;
    }
    if (_isThreadCpuTimeMeasurementEnabled != enable) {
      if (enable) {
        LOGGER.info("Enabling thread CPU time measurement");
      } else {
        LOGGER.info("Disabling thread CPU time measurement");
      }
    }
    try {
      MX_BEAN.setThreadCpuTimeEnabled(enable);
      _isThreadCpuTimeMeasurementEnabled = enable;
    } catch (Exception e) {
      LOGGER.error("Caught exception {} thread CPU time measurement", enable ? "enabling" : "disabling", e);
      _isThreadCpuTimeMeasurementEnabled = false;
    }
  }

  public static boolean isThreadMemoryMeasurementEnabled() {
    return _isThreadMemoryMeasurementEnabled;
  }

  public static void setThreadMemoryMeasurementEnabled(boolean enable) {
    if (!IS_THREAD_ALLOCATED_MEMORY_SUPPORTED) {
      assert !_isThreadMemoryMeasurementEnabled;
      if (enable) {
        LOGGER.error("Not enabling thread memory measurement because it is not supported");
      }
      return;
    }
    if (_isThreadMemoryMeasurementEnabled != enable) {
      if (enable) {
        LOGGER.info("Enabling thread memory measurement");
      } else {
        LOGGER.info("Disabling thread memory measurement");
      }
    }
    try {
      SUN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_METHOD.invoke(MX_BEAN, enable);
      _isThreadMemoryMeasurementEnabled = enable;
    } catch (Exception e) {
      LOGGER.error("Caught exception {} thread memory measurement", enable ? "enabling" : "disabling", e);
      _isThreadMemoryMeasurementEnabled = false;
    }
  }

  public static int getThreadCount() {
    return MX_BEAN.getThreadCount();
  }

  public static long getTotalStartedThreadCount() {
    return MX_BEAN.getTotalStartedThreadCount();
  }

  public static long getCurrentThreadCpuTime() {
    return _isThreadCpuTimeMeasurementEnabled ? MX_BEAN.getCurrentThreadCpuTime() : 0;
  }

  public static long getCurrentThreadAllocatedBytes() {
    if (!_isThreadMemoryMeasurementEnabled) {
      return 0;
    }
    if (SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_METHOD != null) {
      try {
        return (long) SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_METHOD.invoke(MX_BEAN);
      } catch (Exception e) {
        LOGGER.error("Caught exception invoking method: {}", SUN_GET_CURRENT_THREAD_ALLOCATED_BYTES_NAME, e);
        return 0;
      }
    } else {
      assert SUN_GET_THREAD_ALLOCATED_BYTES_METHOD != null;
      try {
        return (long) SUN_GET_THREAD_ALLOCATED_BYTES_METHOD.invoke(MX_BEAN, Thread.currentThread().getId());
      } catch (Exception e) {
        LOGGER.error("Caught exception invoking method: {}", SUN_GET_THREAD_ALLOCATED_BYTES_NAME, e);
        return 0;
      }
    }
  }

  /// Returns an approximation of the total garbage collection time in milliseconds.
  public static long getGcTime() {
    long totalGCTime = 0;
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      long gcTime = gcBean.getCollectionTime();
      if (gcTime > 0) {
        totalGCTime += gcTime;
      }
    }
    return totalGCTime;
  }
}
