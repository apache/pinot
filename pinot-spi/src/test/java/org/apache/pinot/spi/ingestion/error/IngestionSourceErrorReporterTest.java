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
package org.apache.pinot.spi.ingestion.error;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IngestionSourceErrorReporterTest {

  @Test
  public void reportReachesOnlyTheListenerOfTheCallingSite()
      throws Exception {
    List<Throwable> outer = new ArrayList<>();
    List<Throwable> inner = new ArrayList<>();
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second");
    RuntimeException third = new RuntimeException("third");
    try (IngestionSourceErrorReporter.Scope ignored = IngestionSourceErrorReporter.listen(outer::add)) {
      IngestionSourceErrorReporter.report(first);
      try (IngestionSourceErrorReporter.Scope nested = IngestionSourceErrorReporter.listen(inner::add)) {
        IngestionSourceErrorReporter.report(second);
      }
      // Another thread's report never reaches this site.
      Thread other = new Thread(() -> IngestionSourceErrorReporter.report(new RuntimeException("other thread")));
      other.start();
      other.join();
      IngestionSourceErrorReporter.report(third);
    }
    // Once the site's call returns, a report goes nowhere.
    IngestionSourceErrorReporter.report(new RuntimeException("after"));
    assertEquals(outer, List.of(first, third));
    assertEquals(inner, List.of(second));
  }

  @Test
  public void failingListenerNeverReachesThePlugin() {
    boolean[] reached = new boolean[1];
    try (IngestionSourceErrorReporter.Scope ignored = IngestionSourceErrorReporter.listen(failure -> {
      reached[0] = true;
      throw new IllegalStateException("listener failed");
    })) {
      IngestionSourceErrorReporter.report(new RuntimeException("swallowed by the plugin"));
    }
    assertTrue(reached[0]);
  }
}
