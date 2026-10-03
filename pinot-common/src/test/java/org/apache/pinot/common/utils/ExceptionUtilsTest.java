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
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for [ExceptionUtils#appendCauses(String, Throwable, int, int)].
public class ExceptionUtilsTest {
  private static final int MAX_CAUSES = 5;
  private static final int MAX_CAUSE_LENGTH = 1024;

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
  public void testAppendCausesSkipsCauseThatTheMessageAlreadyContains() {
    Exception cause = new IOException("Connection refused");

    assertEquals(append("Failed to fetch segment: Connection refused", cause),
        "Failed to fetch segment: Connection refused");
  }

  @Test
  public void testAppendCausesSkipsCauseThatAnEarlierCauseAlreadyContains() {
    Exception cause = new IllegalStateException("Failed to read schema: Unexpected character at line 3",
        new IllegalStateException("Unexpected character at line 3", new IOException("Stream closed")));

    assertEquals(append("Invalid schema", cause),
        "Invalid schema -> Failed to read schema: Unexpected character at line 3 -> Stream closed");
  }

  @Test
  public void testAppendCausesCollapsesRepeatedMessages() {
    Exception cause = new IllegalStateException("Segment push failed",
        new IllegalStateException("Segment push failed", new IOException("Connection reset")));

    assertEquals(append("Segment push failed", cause), "Segment push failed -> Connection reset");
  }

  @Test
  public void testAppendCausesDropsRepeatsBeforeApplyingTheBound() {
    Exception cause = new IllegalStateException("A", new IllegalStateException("B", new IllegalStateException("C",
        new IllegalStateException("D", new IllegalStateException("M", new IllegalStateException("M"))))));

    assertEquals(append("top", cause), "top -> A -> B -> C -> D -> M");
  }

  @Test
  public void testAppendCausesDropsCausesThatThePreviousOneSaysBeforeApplyingTheBound() {
    Exception cause = new IllegalStateException("Failed to read row: X", new IllegalStateException("X",
        new IllegalStateException("Y", new IllegalStateException("Z", new IllegalStateException("root")))));

    assertEquals(ExceptionUtils.appendCauses("top", cause, 4, MAX_CAUSE_LENGTH),
        "top -> Failed to read row: X -> Y -> Z -> root");
  }

  @Test
  public void testAppendCausesMatchesWholeMessagesOnly() {
    assertEquals(append("Failed to load table_5", new IOException("5")), "Failed to load table_5 -> 5");
    assertEquals(append("Failed to load table: foo", new IOException("foo_bar")),
        "Failed to load table: foo -> foo_bar");
    assertEquals(append("Retry limit reached: 5", new IOException("5")), "Retry limit reached: 5");
    assertEquals(append("Invalid value: (5)", new IOException("(5)")), "Invalid value: (5)");
  }

  @Test
  public void testAppendCausesKeepsRootCauseWhenAnEarlierMessageRepeatingItIsAbbreviated() {
    String rootCause = "Cannot read single-value from Object[]: [a, b] for column: v";
    String wrapperMessage = StringUtils.repeat('x', 1000) + rootCause + StringUtils.repeat('y', 1000);
    Exception cause = new RuntimeException(wrapperMessage, new IllegalArgumentException(rootCause));

    String result = append("Failed", cause);

    assertTrue(result.endsWith("y -> " + rootCause), result);
  }

  @DataProvider
  public Object[][] blankMessages() {
    return new Object[][]{{null}, {""}, {"   "}, {"\n\t"}};
  }

  @Test(dataProvider = "blankMessages")
  public void testAppendCausesSkipsCauseWithoutMessageThatHasACause(String blankMessage) {
    Exception cause = new MessageException(blankMessage, new IOException("inner"));

    assertEquals(append("outer", cause), "outer -> inner");
  }

  @Test(dataProvider = "blankMessages")
  public void testAppendCausesNamesRootCauseWithoutMessageByItsType(String blankMessage) {
    assertEquals(append("Failed to build segment", new MessageException(blankMessage, null)),
        "Failed to build segment -> MessageException");
    assertEquals(append("Failed to build segment", new NullPointerException()),
        "Failed to build segment -> NullPointerException");
  }

  @Test
  public void testAppendCausesNamesAnonymousRootCauseByItsClassName() {
    Exception cause = new RuntimeException() {
    };

    assertEquals(append("Failed", cause), "Failed -> " + cause.getClass().getName());
  }

  @Test
  public void testAppendCausesSkipsMessageGeneratedFromCause() {
    Exception cause = new RuntimeException(new RuntimeException(new IllegalArgumentException("Invalid column: foo")));

    assertEquals(append("Failed to build segment", cause), "Failed to build segment -> Invalid column: foo");
  }

  @Test
  public void testAppendCausesSkipsMessageGeneratedFromCauseWithoutMessage() {
    Exception cause = new RuntimeException(new NullPointerException());

    assertEquals(append("Failed to build segment", cause), "Failed to build segment -> NullPointerException");
  }

  @Test
  public void testAppendCausesKeepsMessagesVerbatim() {
    assertEquals(append(" outer ", new IOException(" inner\n")), " outer  ->  inner\n");
  }

  @Test
  public void testAppendCausesAbbreviatesLongMessagesInTheMiddle() {
    String longValue = StringUtils.repeat('x', 5000);
    Exception cause =
        new IllegalArgumentException("Cannot read single-value from Object[]: [" + longValue + "] for column: v");

    String result = ExceptionUtils.appendCauses("Caught exception while reading data", cause, MAX_CAUSES, 100);

    String[] parts = result.split(" -> ");
    assertEquals(parts.length, 2);
    assertEquals(parts[0], "Caught exception while reading data");
    assertEquals(parts[1].length(), 100);
    assertTrue(parts[1].startsWith("Cannot read single-value from Object[]: [xxx"), parts[1]);
    assertTrue(parts[1].contains("xxx...xxx"), parts[1]);
    assertTrue(parts[1].endsWith("xxx] for column: v"), parts[1]);
  }

  @Test
  public void testAppendCausesDoesNotAbbreviateTheMessage() {
    String message = StringUtils.repeat('x', 200);

    assertEquals(ExceptionUtils.appendCauses(message, new IOException("inner"), MAX_CAUSES, 10), message + " -> inner");
  }

  @Test
  public void testAppendCausesKeepsCauseAtExactlyTheMaximumLength() {
    String causeMessage = StringUtils.repeat('x', 100);

    assertEquals(ExceptionUtils.appendCauses("m", new IOException(causeMessage), MAX_CAUSES, 100),
        "m -> " + causeMessage);
  }

  @Test
  public void testAppendCausesDoesNotSplitASurrogatePair() {
    String causeMessage = "ab😀cdefghij😀kl";

    String result = ExceptionUtils.appendCauses("m", new IOException(causeMessage), MAX_CAUSES, 9);

    assertEquals(result, "m -> ab...kl");
  }

  @Test
  public void testAppendCausesKeepsOutermostAndInnermostCausesOfADeepChain() {
    Throwable cause = null;
    for (int i = 19; i >= 0; i--) {
      cause = new RuntimeException("cause-" + i, cause);
    }

    assertEquals(append("top", cause), "top -> cause-0 -> ... -> cause-16 -> cause-17 -> cause-18 -> cause-19");
  }

  @DataProvider
  public Object[][] maxCauses() {
    return new Object[][]{
        {1, "top -> ... -> cause-5"},
        {2, "top -> cause-0 -> ... -> cause-5"},
        {5, "top -> cause-0 -> ... -> cause-2 -> cause-3 -> cause-4 -> cause-5"},
        {6, "top -> cause-0 -> cause-1 -> cause-2 -> cause-3 -> cause-4 -> cause-5"},
        {7, "top -> cause-0 -> cause-1 -> cause-2 -> cause-3 -> cause-4 -> cause-5"}
    };
  }

  @Test(dataProvider = "maxCauses")
  public void testAppendCausesBoundsTheNumberOfCauses(int maxCauses, String expected) {
    Throwable cause = null;
    for (int i = 5; i >= 0; i--) {
      cause = new RuntimeException("cause-" + i, cause);
    }

    assertEquals(ExceptionUtils.appendCauses("top", cause, maxCauses, MAX_CAUSE_LENGTH), expected);
  }

  @Test
  public void testAppendCausesCountsOnlyKeptCausesAgainstTheBound() {
    Throwable cause = new IOException("root cause");
    for (int i = 0; i < 10; i++) {
      cause = new RuntimeException(cause);
    }

    assertEquals(ExceptionUtils.appendCauses("outer", cause, 1, MAX_CAUSE_LENGTH), "outer -> root cause");
  }

  @Test(timeOut = 10_000)
  public void testAppendCausesTerminatesOnSelfCause() {
    assertEquals(append("top", new SelfCausedException("self")), "top -> self");
  }

  @Test(timeOut = 10_000)
  public void testAppendCausesTerminatesOnCauseCycle() {
    Exception first = new RuntimeException("first");
    Exception second = new IllegalStateException("second");
    first.initCause(second);
    second.initCause(first);

    assertEquals(append("top", first), "top -> first -> second");
  }

  @Test
  public void testAppendCausesIgnoresSuppressedExceptions() {
    Exception cause = new RuntimeException("primary");
    cause.addSuppressed(new IOException("suppressed"));

    assertEquals(append("top", cause), "top -> primary");
  }

  @Test
  public void testAppendCausesRejectsInvalidBounds() {
    Exception cause = new RuntimeException("message");

    expectThrows(IllegalArgumentException.class, () -> ExceptionUtils.appendCauses("m", cause, 0, MAX_CAUSE_LENGTH));
    expectThrows(IllegalArgumentException.class, () -> ExceptionUtils.appendCauses("m", cause, -1, MAX_CAUSE_LENGTH));
    expectThrows(IllegalArgumentException.class, () -> ExceptionUtils.appendCauses("m", cause, MAX_CAUSES, 4));
    assertEquals(ExceptionUtils.appendCauses("m", cause, 1, 5), "m -> m...e");
  }

  private static String append(String message, Throwable cause) {
    return ExceptionUtils.appendCauses(message, cause, MAX_CAUSES, MAX_CAUSE_LENGTH);
  }

  private static class MessageException extends Exception {
    MessageException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class SelfCausedException extends RuntimeException {
    SelfCausedException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }
}
