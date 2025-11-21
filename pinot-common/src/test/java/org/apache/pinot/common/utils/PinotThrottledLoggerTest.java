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
package org.apache.pinot.common.utils;

import java.io.IOException;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class PinotThrottledLoggerTest {

  @Test
  public void testIndependentRateLimitingPerExceptionClass() {
    Logger mockLogger = mock(Logger.class);
    PinotThrottledLogger throttledLogger = new PinotThrottledLogger(mockLogger, 2.0 / 60.0);

    for (int i = 0; i < 100; i++) {
      throttledLogger.warn("Error", new NumberFormatException("Invalid number"));
    }
    for (int i = 0; i < 100; i++) {
      throttledLogger.warn("Error", new IllegalArgumentException("Invalid argument"));
    }
    for (int i = 0; i < 100; i++) {
      throttledLogger.error("Error", new IOException("IO error"));
    }

    verify(mockLogger, atMost(3)).warn(eq("Error"), any(NumberFormatException.class));
    verify(mockLogger, atMost(3)).warn(eq("Error"), any(IllegalArgumentException.class));
    verify(mockLogger, atMost(3)).error(eq("Error"), any(IOException.class));
  }

  @Test
  public void testDefaultIngestionConfigDisablesThrottling() {
    Logger mockLogger = mock(Logger.class);
    PinotThrottledLogger throttledLogger = new PinotThrottledLogger(mockLogger, null, null);

    for (int i = 0; i < 100; i++) {
      throttledLogger.warn("Error", new NumberFormatException("Invalid number"));
    }

    verify(mockLogger, never()).warn(anyString(), any(Throwable.class));
    verify(mockLogger, times(100)).debug(eq("Error"), any(NumberFormatException.class));
  }

  @Test
  public void testExplicitZeroRateLimitDisablesThrottling() {
    Logger mockLogger = mock(Logger.class);
    IngestionConfig config = new IngestionConfig();
    config.setIngestionExceptionLogRateLimitPerMin(0);
    PinotThrottledLogger throttledLogger = new PinotThrottledLogger(mockLogger, config, null);

    for (int i = 0; i < 100; i++) {
      throttledLogger.warn("Error", new NumberFormatException("Invalid number"));
    }

    verify(mockLogger, never()).warn(anyString(), any(Throwable.class));
    verify(mockLogger, times(100)).debug(eq("Error"), any(NumberFormatException.class));
  }
}
