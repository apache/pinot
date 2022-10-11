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
package org.apache.pinot.common.request.context;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ThreadTimer} class providing the functionality of measuring the CPU time for the current thread.
 */
public class ThreadTimer {
  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean IS_CURRENT_THREAD_CPU_TIME_SUPPORTED = MX_BEAN.isCurrentThreadCpuTimeSupported();
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadTimer.class);
  private static boolean _isThreadCpuTimeMeasurementEnabled = false;
  private final long _startTimeNs;

  public ThreadTimer() {
    _startTimeNs = _isThreadCpuTimeMeasurementEnabled ? MX_BEAN.getCurrentThreadCpuTime() : -1;
  }

  public static void setThreadCpuTimeMeasurementEnabled(boolean enable) {
    _isThreadCpuTimeMeasurementEnabled = enable && IS_CURRENT_THREAD_CPU_TIME_SUPPORTED;
  }

  public static boolean isThreadCpuTimeMeasurementEnabled() {
    return _isThreadCpuTimeMeasurementEnabled;
  }

  public long getThreadTimeNs() {
    return _isThreadCpuTimeMeasurementEnabled ? MX_BEAN.getCurrentThreadCpuTime() - _startTimeNs : 0;
  }

  static {
    LOGGER.info("Current thread cpu time measurement supported: {}", IS_CURRENT_THREAD_CPU_TIME_SUPPORTED);
  }
}
