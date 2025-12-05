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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ThreadResourceUsageProvider} class providing the functionality of measuring the CPU time
 * and allocateBytes (JVM heap) for the current thread.
 */
public class ThreadResourceUsageProvider {
  private ThreadResourceUsageProvider() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadResourceUsageProvider.class);

  // used for getting the memory allocation function in hotspot jvm through reflection
  private static final String SUN_THREAD_MXBEAN_CLASS_NAME = "com.sun.management.ThreadMXBean";
  private static final String SUN_THREAD_MXBEAN_IS_THREAD_ALLOCATED_MEMORY_SUPPORTED_NAME
      = "isThreadAllocatedMemorySupported";
  private static final String SUN_THREAD_MXBEAN_IS_THREAD_ALLOCATED_MEMORY_ENABLED_NAME
      = "isThreadAllocatedMemoryEnabled";
  private static final String SUN_THREAD_MXBEAN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_NAME
      = "setThreadAllocatedMemoryEnabled";
  private static final String SUN_THREAD_MXBEAN_GET_BYTES_ALLOCATED_NAME = "getThreadAllocatedBytes";
  private static final Method SUN_THREAD_MXBEAN_GET_BYTES_ALLOCATED_METHOD;

  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean IS_CURRENT_THREAD_CPU_TIME_SUPPORTED = MX_BEAN.isCurrentThreadCpuTimeSupported();
  private static final boolean IS_THREAD_ALLOCATED_MEMORY_SUPPORTED;
  private static final boolean IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT;
  private static boolean _isThreadCpuTimeMeasurementEnabled = false;
  private static boolean _isThreadMemoryMeasurementEnabled = false;

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
    try {
      return _isThreadMemoryMeasurementEnabled ? (long) SUN_THREAD_MXBEAN_GET_BYTES_ALLOCATED_METHOD
          .invoke(MX_BEAN, Thread.currentThread().getId()) : 0;
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOGGER.error("Exception happened during the invocation of getting current bytes allocated", e);
      return 0;
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

  public static boolean isThreadCpuTimeMeasurementEnabled() {
    return _isThreadCpuTimeMeasurementEnabled;
  }

  public static void setThreadCpuTimeMeasurementEnabled(boolean enable) {
    _isThreadCpuTimeMeasurementEnabled = enable && IS_CURRENT_THREAD_CPU_TIME_SUPPORTED;
  }

  public static boolean isThreadMemoryMeasurementEnabled() {
    return _isThreadMemoryMeasurementEnabled;
  }

  public static void setThreadMemoryMeasurementEnabled(boolean enable) {

    boolean isThreadAllocateMemoryEnabled = IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT;
    try {
      Class<?> sunThreadMXBeanClass = Class.forName(SUN_THREAD_MXBEAN_CLASS_NAME);
      sunThreadMXBeanClass.getMethod(SUN_THREAD_MXBEAN_SET_THREAD_ALLOCATED_MEMORY_ENABLED_NAME, Boolean.TYPE)
          .invoke(MX_BEAN, enable);
      isThreadAllocateMemoryEnabled = (boolean) sunThreadMXBeanClass
          .getMethod(SUN_THREAD_MXBEAN_IS_THREAD_ALLOCATED_MEMORY_ENABLED_NAME)
          .invoke(MX_BEAN);
    } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      LOGGER.error("Not able to call isThreadAllocatedMemoryEnabled or setThreadAllocatedMemoryEnabled, ", e);
    }
    _isThreadMemoryMeasurementEnabled = enable && IS_THREAD_ALLOCATED_MEMORY_SUPPORTED && isThreadAllocateMemoryEnabled;
  }

  /**
   * Lightweight self-test to verify whether CPU time and allocated-bytes measurements are usable in the
   * current JVM after {@code setThread*MeasurementEnabled(...)} calls.
   */
  public static MeasurementStatus selfTest() {
    boolean cpuTimeUsable = false;
    if (_isThreadCpuTimeMeasurementEnabled) {
      long startCpu = MX_BEAN.getCurrentThreadCpuTime();
      long accumulator = 0;
      for (int i = 0; i < 1_000; i++) {
        accumulator += i;
      }
      long endCpu = MX_BEAN.getCurrentThreadCpuTime();
      cpuTimeUsable = endCpu > startCpu;
      if (accumulator == Long.MIN_VALUE) {
        LOGGER.debug("Unreachable guard to prevent JIT elimination");
      }
    }

    boolean allocatedBytesUsable = false;
    if (_isThreadMemoryMeasurementEnabled) {
      long startAllocated = getCurrentThreadAllocatedBytes();
      byte[] padding = new byte[1_024];
      long endAllocated = getCurrentThreadAllocatedBytes();
      allocatedBytesUsable = endAllocated > startAllocated;
      if (padding[0] == Byte.MIN_VALUE) {
        LOGGER.debug("Unreachable guard to prevent JIT elimination");
      }
    }

    return new MeasurementStatus(cpuTimeUsable, allocatedBytesUsable);
  }

  public static final class MeasurementStatus {
    private final boolean _cpuTimeUsable;
    private final boolean _allocatedBytesUsable;

    public MeasurementStatus(boolean cpuTimeUsable, boolean allocatedBytesUsable) {
      _cpuTimeUsable = cpuTimeUsable;
      _allocatedBytesUsable = allocatedBytesUsable;
    }

    public boolean isCpuTimeUsable() {
      return _cpuTimeUsable;
    }

    public boolean isAllocatedBytesUsable() {
      return _allocatedBytesUsable;
    }
  }

  //initialize the com.sun.management.ThreadMXBean related variables using reflection
  static {
    Class<?> sunThreadMXBeanClass;
    try {
      sunThreadMXBeanClass = Class.forName(SUN_THREAD_MXBEAN_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      LOGGER.error("Not able to load com.sun.management.ThreadMXBean, you are probably not using Hotspot jvm");
      sunThreadMXBeanClass = null;
    }

    boolean isThreadAllocateMemorySupported = false;
    try {
      isThreadAllocateMemorySupported =
          sunThreadMXBeanClass != null && (boolean) sunThreadMXBeanClass
              .getMethod(SUN_THREAD_MXBEAN_IS_THREAD_ALLOCATED_MEMORY_SUPPORTED_NAME)
              .invoke(MX_BEAN);
    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      LOGGER.error("Not able to call isThreadAllocatedMemorySupported, ", e);
    }
    IS_THREAD_ALLOCATED_MEMORY_SUPPORTED = isThreadAllocateMemorySupported;

    boolean isThreadAllocateMemoryEnabled = false;
    try {
      isThreadAllocateMemoryEnabled =
          sunThreadMXBeanClass != null && (boolean) sunThreadMXBeanClass
              .getMethod(SUN_THREAD_MXBEAN_IS_THREAD_ALLOCATED_MEMORY_ENABLED_NAME)
              .invoke(MX_BEAN);
    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      LOGGER.error("Not able to call isThreadAllocatedMemoryEnabled, ", e);
    }
    IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT = isThreadAllocateMemoryEnabled;

    Method threadAllocateBytes = null;
    if (IS_THREAD_ALLOCATED_MEMORY_SUPPORTED) {
      try {
        threadAllocateBytes = sunThreadMXBeanClass
            .getMethod(SUN_THREAD_MXBEAN_GET_BYTES_ALLOCATED_NAME, long.class);
      } catch (NoSuchMethodException ignored) {
      }
    }
    SUN_THREAD_MXBEAN_GET_BYTES_ALLOCATED_METHOD = threadAllocateBytes;
  }

  static {
    LOGGER.info("Current thread cpu time measurement supported: {}", IS_CURRENT_THREAD_CPU_TIME_SUPPORTED);
    LOGGER.info("Current thread allocated bytes measurement supported: {}", IS_THREAD_ALLOCATED_MEMORY_SUPPORTED);
    LOGGER.info("Current thread allocated bytes measurement enabled default: {}",
        IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT);
  }
}
