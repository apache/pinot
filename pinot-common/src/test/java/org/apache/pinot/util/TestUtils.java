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
package org.apache.pinot.util;

import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Various utilities for unit tests.
 *
 */
public class TestUtils {
  private TestUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  public static String getFileFromResourceUrl(@Nonnull URL resourceUrl) {
    // For maven cross package use case, we need to extract the resource from jar to a temporary directory.
    String resourceUrlStr = resourceUrl.toString();
    if (resourceUrlStr.contains("jar!")) {
      try {
        String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
        File tempFile = File.createTempFile("pinot-test-temp", extension);
        String tempFilePath = tempFile.getAbsolutePath();
        LOGGER.info("Extracting from " + resourceUrlStr + " to " + tempFilePath);
        FileUtils.copyURLToFile(resourceUrl, tempFile);
        return tempFilePath;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return resourceUrl.getFile();
    }
  }

  /**
   * Ensure the given directories exist and are empty.
   *
   * @param dirs Directories to be cleared
   * @throws IOException
   */
  public static void ensureDirectoriesExistAndEmpty(@Nonnull File... dirs)
      throws IOException {
    for (File dir : dirs) {
      FileUtils.deleteDirectory(dir);
      Assert.assertTrue(dir.mkdirs());
    }
  }

  /**
   * Wait for a condition to be met.
   *
   * @param condition Condition to be met
   * @param checkIntervalMs Check interval in milliseconds
   * @param timeoutMs Timeout in milliseconds
   * @param errorMessage Error message if condition is not met before timed out
   */
  public static void waitForCondition(Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage) {
    waitForCondition(condition, checkIntervalMs, timeoutMs, errorMessage, true);
  }

  public static void waitForCondition(Function<Void, Boolean> condition, long timeoutMs,
      @Nullable String errorMessage) {
    waitForCondition(condition, 100L, timeoutMs, errorMessage);
  }

  public static void waitForCondition(Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage, boolean raiseError) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    String errorMessageSuffix = errorMessage != null ? ", error message: " + errorMessage : "";
    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.apply(null))) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (Exception e) {
        Assert.fail("Caught exception while checking the condition" + errorMessageSuffix, e);
      }
    }
    if (raiseError) {
      Assert.fail("Failed to meet condition in " + timeoutMs + "ms" + errorMessageSuffix);
    }
  }

  /**
   * Like {@link #waitForCondition(Function, long, long, String, boolean)}, but supports functions that throw checked
   * exceptions.
   *
   * The first time every logPeriod that the function throws an exception, it is printed in the standard error.
   */
  public static void waitForCondition(SupplierWithException<Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage, boolean raiseError, @Nullable Duration logPeriod) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    Instant errorLogInstant = Instant.EPOCH;
    String errorMessageSuffix = errorMessage != null ? ", error message: " + errorMessage : "";
    Throwable lastError = null;
    int numErrors = 0;
    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.get())) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (InterruptedException e) {
        lastError = e;
        break;
      } catch (Exception e) {
        lastError = e;
        if (logPeriod != null) {
          numErrors++;
          Instant now = Instant.now();
          if (logPeriod.compareTo(Duration.between(errorLogInstant, now)) < 0) {
            LOGGER.warn("Error while waiting for condition", e);
            errorLogInstant = now;
          }
        }
      }
    }
    if (raiseError) {
      Assert.fail("Failed to meet condition in " + timeoutMs + "ms after " + numErrors + " errors"
          + errorMessageSuffix, lastError);
    }
  }

  /**
   * Wait for a result to be returned
   *
   * @param supplier result value supplier
   * @param timeoutMs timeout
   * @return result value (non-throwable)
   * @throws InterruptedException if {@code Thread.sleep()} is interrupted
   */
  public static <T> T waitForResult(SupplierWithException<T> supplier, long timeoutMs)
      throws InterruptedException {
    return waitForResult(supplier, timeoutMs, null);
  }

  /**
   * Like {@link #waitForResult(SupplierWithException, long)}, but optionally prints in stderr the first error found
   * every given log period.
   * @param supplier result value supplier
   * @param timeoutMs timeout
   * @param logPeriod how often logs will be printed. The longer the duration, the fewer the logs.
   * @return result value (non-throwable)
   * @throws InterruptedException if {@code Thread.sleep()} is interrupted
   */
  public static <T> T waitForResult(SupplierWithException<T> supplier, long timeoutMs, @Nullable Duration logPeriod)
      throws InterruptedException {
    long tEnd = System.currentTimeMillis() + timeoutMs;
    Instant lastError = Instant.now();
    while (tEnd > System.currentTimeMillis()) {
      try {
        return supplier.get();
      } catch (InterruptedException e) {
        throw new IllegalStateException("Waiting interrupted after "
            + (System.currentTimeMillis() + timeoutMs - tEnd) + "ms");
      } catch (Throwable t) {
        if (logPeriod != null) {
          Instant now = Instant.now();
          if (logPeriod.compareTo(Duration.between(lastError, now)) < 0) {
            t.printStackTrace();
          }
          lastError = now;
        }
      }

      Thread.sleep(1000);
    }

    throw new IllegalStateException("Failed to return result in " + timeoutMs + "ms");
  }

  @FunctionalInterface
  public interface SupplierWithException<T> {
    T get()
        throws Exception;
  }
}
