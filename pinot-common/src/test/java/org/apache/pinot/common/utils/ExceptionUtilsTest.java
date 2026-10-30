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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests for [ExceptionUtils#appendCauses(String, Throwable, int)].
public class ExceptionUtilsTest {
  private static final int MAX_LENGTH = 1024;

  @Test
  public void testAppendCausesReturnsTheMessageWhenThereIsNoCause() {
    String message = "Table: foo_OFFLINE not found";

    assertSame(append(message, null), message);
    assertNull(append(null, null));
  }

  @Test
  public void testAppendCausesListsCausesOutermostFirst() {
    Exception cause = new IllegalStateException("middle", new IOException("inner"));

    assertEquals(append("outer", cause), "outer -> middle -> inner");
  }

  /// The `/ingestFromFile` response for a record that cannot be read: the wrapper message already ends with the first
  /// cause, so only the two below it are added.
  @Test
  public void testAppendCausesKeepsTheRootCauseOfASegmentCreationFailure() {
    Exception rootCause = new IllegalArgumentException(
        "Cannot read single-value from Object[]: [before, after] for column: New_Value");
    Exception transformFailure =
        new RuntimeException("Caught exception while transforming data type for column: New_Value", rootCause);
    Exception readFailure = new RuntimeException("Caught exception while reading data", transformFailure);
    String message = "Caught exception when ingesting file into table: foo_OFFLINE. " + readFailure.getMessage();

    assertEquals(append(message, readFailure), message
        + " -> Caught exception while transforming data type for column: New_Value"
        + " -> Cannot read single-value from Object[]: [before, after] for column: New_Value");
  }

  @Test
  public void testAppendCausesWithoutMessageStartsWithTheFirstCause() {
    Exception cause = new IllegalStateException("middle", new IOException("inner"));

    assertEquals(append(null, cause), "middle -> inner");
    assertEquals(append("", cause), "middle -> inner");
  }

  @Test
  public void testAppendCausesSkipsMessagesTheTextAlreadyContains() {
    Exception cause = new IllegalStateException("Segment push failed", new IllegalStateException(
        "Failed to read schema: Unexpected character at line 3",
        new IllegalStateException("Unexpected character at line 3", new IOException("Connection reset"))));

    assertEquals(append("Segment push failed", cause),
        "Segment push failed -> Failed to read schema: Unexpected character at line 3 -> Connection reset");
  }

  @Test
  public void testAppendCausesSkipsBlankMessagesAndMessagesGeneratedFromTheCause() {
    Exception cause = new RuntimeException(
        new RuntimeException(new IllegalStateException(" ", new IllegalArgumentException("Invalid column: foo"))));

    assertEquals(append("Failed to build segment", cause), "Failed to build segment -> Invalid column: foo");
    assertEquals(append("Failed to build segment", new RuntimeException(new NullPointerException())),
        "Failed to build segment");
  }

  @Test
  public void testAppendCausesKeepsAMessageThatOnlyLooksGeneratedFromTheCause() {
    Exception cause = new IllegalStateException("java.lang.IllegalArgumentException: something else",
        new IllegalArgumentException("root"));

    assertEquals(append("top", cause), "top -> java.lang.IllegalArgumentException: something else -> root");
  }

  @Test
  public void testAppendCausesAbbreviatesTheCausesInTheMiddleButNotTheMessage() {
    String message = StringUtils.repeat('m', 200);
    String rootCause = "Cannot read single-value from Object[]: [a, b] for column: v";
    Exception cause = new IllegalStateException(StringUtils.repeat('x', 5000), new IllegalArgumentException(rootCause));

    String result = ExceptionUtils.appendCauses(message, cause, 200);

    assertEquals(result.length(), message.length() + 200);
    assertTrue(result.startsWith(message + " -> xxx"), result);
    assertTrue(result.contains("x...x"), result);
    assertTrue(result.endsWith("x -> " + rootCause), result);
  }

  @Test(timeOut = 10_000)
  public void testAppendCausesTerminatesOnACauseCycle() {
    Exception first = new RuntimeException("first");
    Exception second = new IllegalStateException("second");
    first.initCause(second);
    second.initCause(first);

    assertEquals(append("top", first), "top -> first -> second");
  }

  @Test
  public void testStackTraceFormatsCausesAndHandlesMissingExceptions() {
    IOException inner = new IOException("inner");
    inner.setStackTrace(new StackTraceElement[]{
        new StackTraceElement("example.Reader", "read", "Reader.java", 20)
    });
    IllegalStateException outer = new IllegalStateException("outer", inner);
    outer.setStackTrace(new StackTraceElement[]{
        new StackTraceElement("example.Worker", "dispatch", "Worker.java", 10)
    });
    String expected = "outer\nexample.Worker.dispatch(Worker.java:10)\n"
        + "inner\nexample.Reader.read(Reader.java:20)\n";

    assertEquals(ExceptionUtils.getStackTrace(outer), expected);
    assertEquals(ExceptionUtils.getStackTrace(outer, 10), expected);
    assertEquals(ExceptionUtils.getStackTrace(outer, 10, 1024), expected);
    assertEquals(ExceptionUtils.getStackTrace(null), "");
  }

  @Test
  public void testStackTraceTruncationKeepsCompleteFramesAndOmitsLaterCauses() {
    IllegalStateException outer = withFourFrames("outer", withFourFrames("inner", null));
    String firstFrame = "outer\nexample.Worker.step0(Worker.java:10)\n";

    // Truncation happens after a complete frame, preserving a usable failure location.
    assertEquals(ExceptionUtils.getStackTrace(outer, 0), firstFrame);
    assertEquals(ExceptionUtils.getStackTrace(outer, Integer.MAX_VALUE, 1), firstFrame);
  }

  @Test
  public void testConsolidatedTracesIncludeCausesAndBoundLongChains() {
    String onlyTrace = "only\nexample.Worker.step0(Worker.java:10)\n"
        + "example.Worker.step1(Worker.java:11)\nexample.Worker.step2(Worker.java:12)\n"
        + "example.Worker.step3(Worker.java:13)\n";
    assertEquals(ExceptionUtils.consolidateExceptionTraces(withFourFrames("only", null)), onlyTrace);
    assertEquals(ExceptionUtils.consolidateExceptionTraces(null), "");

    Throwable failure = null;
    for (int cause = 11; cause >= 0; cause--) {
      failure = withFourFrames("cause" + cause, failure);
    }
    String trace = ExceptionUtils.consolidateExceptionTraces(failure);
    assertEquals(trace.lines().filter(line -> line.startsWith("cause")).collect(Collectors.toList()),
        List.of("cause0", "cause1", "cause2", "cause3", "cause4", "cause5", "cause6", "cause7", "cause8",
            "cause9"));
    assertEquals(trace.lines().count(), 50L);
  }

  private static IllegalStateException withFourFrames(String message, Throwable cause) {
    IllegalStateException exception = new IllegalStateException(message, cause);
    exception.setStackTrace(new StackTraceElement[]{
        new StackTraceElement("example.Worker", "step0", "Worker.java", 10),
        new StackTraceElement("example.Worker", "step1", "Worker.java", 11),
        new StackTraceElement("example.Worker", "step2", "Worker.java", 12),
        new StackTraceElement("example.Worker", "step3", "Worker.java", 13)
    });
    return exception;
  }

  private static String append(String message, Throwable cause) {
    return ExceptionUtils.appendCauses(message, cause, MAX_LENGTH);
  }
}
