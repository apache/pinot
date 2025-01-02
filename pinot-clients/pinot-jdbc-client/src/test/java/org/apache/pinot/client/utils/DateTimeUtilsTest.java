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
package org.apache.pinot.client.utils;

import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DateTimeUtilsTest {

  @Test
  public void testDateFromStringConcurrent()
      throws Throwable {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    for (int i = 0; i < 10; i++) {
      executorService.submit(() -> {
        try {
          Assert.assertEquals(DateTimeUtils.getDateFromString("2020-01-01", Calendar.getInstance()).toString(),
              "2020-01-01");
        } catch (Throwable t) {
          throwable.set(t);
        }
      });
    }

    executorService.shutdown();
    executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);

    if (throwable.get() != null) {
      throw throwable.get();
    }
  }

  @Test
  public void testTimeFromStringConcurrent()
      throws Throwable {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    for (int i = 0; i < 10; i++) {
      executorService.submit(() -> {
        try {
          Assert.assertEquals(DateTimeUtils.getTimeFromString("2020-01-01 12:00:00", Calendar.getInstance()).toString(),
              "12:00:00");
        } catch (Throwable t) {
          throwable.set(t);
        }
      });
    }

    executorService.shutdown();
    executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);

    if (throwable.get() != null) {
      throw throwable.get();
    }
  }

  @Test
  public void testTimestampFromStringConcurrent()
      throws Throwable {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    for (int i = 0; i < 10; i++) {
      executorService.submit(() -> {
        try {
          Assert.assertEquals(
              DateTimeUtils.getTimestampFromString("2020-01-01 12:00:00", Calendar.getInstance()).toString(),
              "2020-01-01 12:00:00.0");
        } catch (Throwable t) {
          throwable.set(t);
        }
      });
    }

    executorService.shutdown();
    executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);

    if (throwable.get() != null) {
      throw throwable.get();
    }
  }

  @Test
  public void testDateFromString() {
    Assert.assertEquals(DateTimeUtils.getDateFromString("2020-01-01", Calendar.getInstance()).toString(), "2020-01-01");
    Assert.assertEquals(
        DateTimeUtils.getDateFromString("2020-01-01 12:00:00", Calendar.getInstance()).toString(),
        "2020-01-01");
    Assert.assertEquals(
        DateTimeUtils.getDateFromString("2020-01-01 12:00:00.000", Calendar.getInstance()).toString(),
        "2020-01-01");
    Assert.assertThrows(() -> DateTimeUtils.getDateFromString("2020-01", Calendar.getInstance()));
  }

  @Test
  public void testTimeFromString() {
    Assert.assertEquals(DateTimeUtils.getTimeFromString("2020-01-01 12:00:00", Calendar.getInstance()).toString(),
        "12:00:00");
    Assert.assertEquals(DateTimeUtils.getTimeFromString("2020-01-01 12:00:00.000", Calendar.getInstance()).toString(),
        "12:00:00");
    Assert.assertThrows(() -> DateTimeUtils.getTimeFromString("2020-01-01", Calendar.getInstance()));
  }

  @Test
  public void testTimestampFromString() {
    Assert.assertEquals(
        DateTimeUtils.getTimestampFromString("2020-01-01 12:00:00", Calendar.getInstance()).toString(),
        "2020-01-01 12:00:00.0");
    Assert.assertEquals(
        DateTimeUtils.getTimestampFromString("2020-01-01 12:00:00.000", Calendar.getInstance()).toString(),
        "2020-01-01 12:00:00.0");
    Assert.assertThrows(() -> DateTimeUtils.getTimestampFromString("2020-01-01", Calendar.getInstance()));
  }
}
