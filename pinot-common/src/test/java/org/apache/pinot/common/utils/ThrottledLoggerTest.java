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
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class ThrottledLoggerTest {

  @Test
  public void testIndependentRateLimitingPerExceptionClass() {
    Logger mockLogger = mock(Logger.class);
    ThrottledLogger throttledLogger = new ThrottledLogger(mockLogger, 2.0 / 60.0);

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
    ThrottledLogger throttledLogger = new ThrottledLogger(mockLogger, null);

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
    ThrottledLogger throttledLogger = new ThrottledLogger(mockLogger, config);

    for (int i = 0; i < 100; i++) {
      throttledLogger.warn("Error", new NumberFormatException("Invalid number"));
    }

    verify(mockLogger, never()).warn(anyString(), any(Throwable.class));
    verify(mockLogger, times(100)).debug(eq("Error"), any(NumberFormatException.class));
  }

  @Test
  public void testDroppedCountReportedWithCorrectMessage() {
    Logger mockLogger = mock(Logger.class);
    ThrottledLogger throttledLogger = new ThrottledLogger(mockLogger, 1.0);

    NumberFormatException exception = new NumberFormatException("Test");
    for (int i = 0; i < 10; i++) {
      throttledLogger.warn("Error occurred", exception);
    }

    verify(mockLogger, times(1)).warn(eq("Error occurred"), eq(exception));
    verify(mockLogger, times(9)).debug(eq("Error occurred"), eq(exception));

    IllegalArgumentException differentException = new IllegalArgumentException("Different");
    throttledLogger.warn("Different error", differentException);

    verify(mockLogger, times(1)).warn(eq("Different error"), eq(differentException));
  }

  @Test
  public void testDroppedCountIndependentPerExceptionClass() {
    Logger mockLogger = mock(Logger.class);
    ThrottledLogger throttledLogger = new ThrottledLogger(mockLogger, 2.0);

    NumberFormatException exceptionA = new NumberFormatException("Test A");
    for (int i = 0; i < 10; i++) {
      throttledLogger.warn("Error A", exceptionA);
    }

    IllegalArgumentException exceptionB = new IllegalArgumentException("Test B");
    for (int i = 0; i < 5; i++) {
      throttledLogger.error("Error B", exceptionB);
    }

    verify(mockLogger, times(2)).warn(eq("Error A"), eq(exceptionA));
    verify(mockLogger, times(8)).debug(eq("Error A"), eq(exceptionA));

    verify(mockLogger, times(2)).error(eq("Error B"), eq(exceptionB));
    verify(mockLogger, times(3)).debug(eq("Error B"), eq(exceptionB));

    verify(mockLogger, times(2)).warn(anyString(), any(NumberFormatException.class));
    verify(mockLogger, times(2)).error(anyString(), any(IllegalArgumentException.class));
  }

  @Test
  public void testNoSuppressionMessageWhenZeroDrops() {
    Logger mockLogger = mock(Logger.class);
    ThrottledLogger throttledLogger = new ThrottledLogger(mockLogger, 10.0);

    NumberFormatException exception = new NumberFormatException("Test");
    throttledLogger.warn("First log", exception);

    verify(mockLogger, times(1)).warn(eq("First log"), eq(exception));
    verify(mockLogger, never()).debug(anyString(), any(Throwable.class));

    throttledLogger.warn("Second log", exception);

    verify(mockLogger, times(1)).warn(eq("Second log"), eq(exception));

    ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockLogger, atLeast(0)).warn(messageCaptor.capture(), any(Throwable.class));

    for (String message : messageCaptor.getAllValues()) {
      assertThat(message).doesNotContain("Dropped");
    }
  }
}
