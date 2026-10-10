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
package org.apache.pinot.controller.api.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.segment.spi.creator.RecordProcessingException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests which /ingestFromFile failures expose their causes.
public class PinotIngestionRestletResourceTest {
  private static final String MESSAGE = "Caught exception when ingesting file into table: testTable_OFFLINE. ";
  private static final String ROOT_CAUSE = "Cannot read single-value from Object[]: [cooper, max] for column: name";
  private static final WebApplicationExceptionMapper MAPPER = new WebApplicationExceptionMapper(true);

  @Test
  public void testRecordFailureIsDescribedByItsCauseChain() {
    Exception e = new RecordProcessingException("Caught exception while reading data",
        new RuntimeException("Caught exception while transforming data type for column: name",
            new IllegalArgumentException(ROOT_CAUSE)));

    ControllerApplicationException failure = getFailure(e);

    assertSame(failure.getCause(), e);
    assertEquals(MAPPER.getErrorMessage(failure), MESSAGE + "Caught exception while reading data"
        + " -> Caught exception while transforming data type for column: name -> " + ROOT_CAUSE);
  }

  @Test
  public void testWrappedRecordFailureIsDescribedByItsCauseChain() {
    Exception e = new IllegalStateException("Failed to build segment",
        new RecordProcessingException("Error occurred while reading row during indexing",
            new IllegalArgumentException(ROOT_CAUSE)));

    assertEquals(MAPPER.getErrorMessage(getFailure(e)),
        MESSAGE + "Failed to build segment -> Error occurred while reading row during indexing -> " + ROOT_CAUSE);
  }

  @Test
  public void testFailureToSetUpARecordReaderIsDescribedByItsMessageAlone() {
    Exception e = new IOException("Failed to create Protobuf descriptor",
        new FileNotFoundException("/etc/controller-secret.desc (Permission denied)"));

    ControllerApplicationException failure = getFailure(e);

    assertNull(failure.getCause());
    String message = MAPPER.getErrorMessage(failure);
    assertEquals(message, MESSAGE + "Failed to create Protobuf descriptor");
    assertFalse(message.contains("controller-secret"), message);
  }

  @Test
  public void testFailureWithoutRecordFailureInItsChainIsDescribedByItsMessageAlone() {
    Exception e = new RuntimeException("Failed to upload segment", new IOException("Connection reset"));

    assertNull(getFailure(e).getCause());
    assertEquals(MAPPER.getErrorMessage(getFailure(e)), MESSAGE + "Failed to upload segment");
  }

  @Test
  public void testIllegalArgumentWithoutRecordFailureIsDescribedByItsMessageAlone() {
    Exception e = new IllegalArgumentException("Invalid record reader config",
        new FileNotFoundException("/etc/controller-secret.desc (No such file or directory)"));

    ControllerApplicationException failure =
        PinotIngestionRestletResource.newIngestFromFileException(MESSAGE + e.getMessage(), Response.Status.BAD_REQUEST,
            e);

    assertNull(failure.getCause());
    assertEquals(failure.getResponse().getStatus(), 400);
    assertEquals(MAPPER.getErrorMessage(failure), MESSAGE + "Invalid record reader config");
  }

  @Test(timeOut = 10_000)
  public void testCyclicChainWithoutRecordFailureIsDescribedByItsMessageAlone() {
    Exception first = new RuntimeException("first");
    Exception second = new IllegalStateException("second");
    first.initCause(second);
    second.initCause(first);

    assertNull(getFailure(first).getCause());
  }

  @Test
  public void testFailureKeepsItsStatus() {
    Exception e = new RecordProcessingException("Caught exception while reading data", new IOException("inner"));

    assertEquals(PinotIngestionRestletResource.newIngestFromFileException(MESSAGE, Response.Status.BAD_REQUEST, e)
        .getResponse().getStatus(), 400);
    assertEquals(getFailure(new IOException("setup")).getResponse().getStatus(), 500);
  }

  @Test
  public void testRecordFailureIsBoundedInLength() {
    String longValue = StringUtils.repeat('x', 10_000);
    Exception e = new RecordProcessingException("Caught exception while reading data",
        new IllegalArgumentException("Cannot read single-value from Object[]: [" + longValue + "] for column: name"));

    String message = MAPPER.getErrorMessage(getFailure(e));

    String prefix = MESSAGE + "Caught exception while reading data -> ";
    assertTrue(message.startsWith(prefix + "Cannot read single-value from Object[]: ["), message);
    assertTrue(message.endsWith("] for column: name"), message);
    assertEquals(message.length(), prefix.length() + WebApplicationExceptionMapper.MAX_CAUSE_LENGTH);
  }

  @Test
  public void testRecordFailureIsBoundedInDepth() {
    Exception e = new IllegalArgumentException(ROOT_CAUSE);
    for (int i = 9; i >= 0; i--) {
      e = new RuntimeException("wrapper-" + i, e);
    }
    e = new RecordProcessingException("Caught exception while reading data", e);

    assertEquals(MAPPER.getErrorMessage(getFailure(e)), MESSAGE + "Caught exception while reading data"
        + " -> wrapper-0 -> ... -> wrapper-7 -> wrapper-8 -> wrapper-9 -> " + ROOT_CAUSE);
  }

  private static ControllerApplicationException getFailure(Exception e) {
    return PinotIngestionRestletResource.newIngestFromFileException(MESSAGE + e.getMessage(),
        Response.Status.INTERNAL_SERVER_ERROR, e);
  }
}
