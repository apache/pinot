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
package org.apache.pinot.connector.flink.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotSinkWriterTest {
  @Test
  public void testFailedUploadsAreDrainedAfterReportingFailure()
      throws Exception {
    IOException firstFailure = new IOException("upload failed");
    IOException secondFailure = new IOException("another upload failed");
    List<Future<?>> pendingUploads = new ArrayList<>();
    pendingUploads.add(CompletableFuture.failedFuture(new RuntimeException("upload task failed", firstFailure)));
    pendingUploads.add(CompletableFuture.failedFuture(new RuntimeException("upload task failed", secondFailure)));

    IOException failure = expectThrows(IOException.class, () -> PinotSink.awaitPendingUploads(pendingUploads));

    assertSame(failure, firstFailure);
    assertEquals(failure.getSuppressed().length, 1);
    assertSame(failure.getSuppressed()[0], secondFailure);
    assertTrue(pendingUploads.isEmpty(), "Failed futures must not be retried during writer close");
    PinotSink.awaitPendingUploads(pendingUploads);
  }
}
