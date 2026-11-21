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
package org.apache.pinot.segment.local.realtime;

import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.realtime.RealtimeIngestionRecordProcessor.Listener;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline.Result;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests record outcomes shared by live consumption and native fixed-range re-ingestion.
public class RealtimeIngestionRecordProcessorTest {
  private static final StreamMessageMetadata METADATA = new StreamMessageMetadata.Builder()
      .setOffset(new LongMsgOffset(0), new LongMsgOffset(1)).setSerializedValueSize(32).build();

  @Test
  public void testExpandedMessageContinuesAfterIndexFailure()
      throws Exception {
    MutableSegment segment = mock(MutableSegment.class);
    Listener listener = mock(Listener.class);
    RealtimeIngestionRecordProcessor processor = createProcessor(segment, listener, false);
    GenericRow first = row(1);
    GenericRow invalid = row(2);
    GenericRow last = row(3);
    RuntimeException failure = new IllegalArgumentException("Invalid value");
    when(segment.index(first, METADATA)).thenReturn(true);
    when(segment.index(invalid, METADATA)).thenThrow(failure);
    when(segment.index(last, METADATA)).thenReturn(false);

    GenericRow message = new GenericRow();
    message.putValue(GenericRow.MULTIPLE_RECORDS_KEY, List.of(first, invalid, last));
    assertFalse(processor.process(decoded(message), METADATA));
    assertEquals(processor.getNumRowsIndexed(), 2);

    InOrder order = inOrder(segment, listener);
    order.verify(listener).onTransformResult(any(Result.class), eq(METADATA));
    order.verify(segment).index(first, METADATA);
    order.verify(listener).onRowIndexed(first, METADATA);
    order.verify(segment).index(invalid, METADATA);
    order.verify(listener).onIndexError(invalid, METADATA, failure);
    order.verify(segment).index(last, METADATA);
    order.verify(listener).onRowIndexed(last, METADATA);
  }

  @Test
  public void testTrailingIndexFailurePreservesLastCapacitySignal()
      throws Exception {
    MutableSegment segment = mock(MutableSegment.class);
    Listener listener = mock(Listener.class);
    RealtimeIngestionRecordProcessor processor = createProcessor(segment, listener, false);
    GenericRow first = row(1);
    GenericRow invalid = row(2);
    when(segment.index(first, METADATA)).thenReturn(false);
    when(segment.index(invalid, METADATA)).thenThrow(new IllegalArgumentException("Invalid value"));

    GenericRow message = new GenericRow();
    message.putValue(GenericRow.MULTIPLE_RECORDS_KEY, List.of(first, invalid));
    assertFalse(processor.process(decoded(message), METADATA));
    assertEquals(processor.getNumRowsIndexed(), 1);
    verify(listener, never()).onRowIndexed(invalid, METADATA);
  }

  @Test
  public void testTransformFailureDoesNotPreventFollowingMessage()
      throws Exception {
    MutableSegment segment = mock(MutableSegment.class);
    Listener listener = mock(Listener.class);
    TransformPipeline pipeline = spy(TransformPipeline.getPassThroughPipeline("test_REALTIME"));
    RealtimeIngestionRecordProcessor processor =
        new RealtimeIngestionRecordProcessor(pipeline, segment, false, listener);
    GenericRow invalid = row(1);
    GenericRow valid = row(2);
    RuntimeException failure = new IllegalArgumentException("Invalid transform input");
    doThrow(failure).when(pipeline).processRow(invalid);
    when(segment.index(valid, METADATA)).thenReturn(true);

    assertTrue(processor.process(decoded(invalid), METADATA));
    assertEquals(processor.getNumRowsIndexed(), 0);
    verify(listener).onTransformError(invalid, METADATA, failure);
    verifyNoInteractions(segment);

    assertTrue(processor.process(decoded(valid), METADATA));
    assertEquals(processor.getNumRowsIndexed(), 1);
    verify(listener).onRowIndexed(valid, METADATA);
  }

  @DataProvider
  public Object[][] decodePolicies() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "decodePolicies")
  public void testTerminalDecodeErrorPolicyAndIndexedCountReset(boolean fatal)
      throws Exception {
    MutableSegment segment = mock(MutableSegment.class);
    Listener listener = mock(Listener.class);
    RealtimeIngestionRecordProcessor processor = createProcessor(segment, listener, fatal);
    GenericRow valid = row(1);
    when(segment.index(valid, METADATA)).thenReturn(true);
    assertTrue(processor.process(decoded(valid), METADATA));
    assertEquals(processor.getNumRowsIndexed(), 1);

    RuntimeException cause = new IllegalArgumentException("Cannot decode record");
    StreamDataDecoderResult error = new StreamDataDecoderResult(null, cause);
    if (fatal) {
      RuntimeException thrown = expectThrows(RuntimeException.class, () -> processor.process(error, METADATA));
      assertSame(thrown.getCause(), cause);
    } else {
      assertTrue(processor.process(error, METADATA));
    }
    assertEquals(processor.getNumRowsIndexed(), 0);
    verify(listener).onDecodeError(cause, METADATA, fatal);
    verify(segment, times(1)).index(any(GenericRow.class), eq(METADATA));
  }

  @Test
  public void testTransformStatisticsAndFilteredMessagesAreReported()
      throws Exception {
    MutableSegment segment = mock(MutableSegment.class);
    Listener listener = mock(Listener.class);
    RealtimeIngestionRecordProcessor processor = createProcessor(segment, listener, false);
    GenericRow valid = row(1);
    valid.markIncomplete();
    valid.markSanitized();
    when(segment.index(valid, METADATA)).thenReturn(true);
    assertTrue(processor.process(decoded(valid), METADATA));
    GenericRow skipped = new GenericRow();
    skipped.putValue(GenericRow.SKIP_RECORD_KEY, true);
    assertTrue(processor.process(decoded(skipped), METADATA));
    assertEquals(processor.getNumRowsIndexed(), 0);

    ArgumentCaptor<Result> results = ArgumentCaptor.forClass(Result.class);
    verify(listener, times(2)).onTransformResult(results.capture(), eq(METADATA));
    assertEquals(results.getAllValues().get(0).getIncompleteRowCount(), 1);
    assertEquals(results.getAllValues().get(0).getSanitizedRowCount(), 1);
    assertEquals(results.getAllValues().get(1).getSkippedRowCount(), 1);
    verify(segment, never()).index(skipped, METADATA);
    verify(listener, never()).onRowIndexed(skipped, METADATA);
  }

  @DataProvider
  public Object[][] stopOnDecodeErrorConfigurations() {
    return new Object[][]{{null, false}, {"false", false}, {"FALSE", false}, {"true", true}, {"TRUE", true},
        {"invalid", false}};
  }

  @Test(dataProvider = "stopOnDecodeErrorConfigurations")
  public void testStopOnDecodeErrorConfiguration(String configured, boolean expected) {
    StreamConfig config = mock(StreamConfig.class);
    when(config.getStreamConfigsMap())
        .thenReturn(configured == null ? Map.of() : Map.of("stopOnDecodeError", configured));
    assertEquals(RealtimeIngestionRecordProcessor.parseStopOnDecodeErrorConfig(config, mock(Logger.class)), expected);
  }

  private static RealtimeIngestionRecordProcessor createProcessor(MutableSegment segment, Listener listener,
      boolean stopOnDecodeError) {
    return new RealtimeIngestionRecordProcessor(TransformPipeline.getPassThroughPipeline("test_REALTIME"), segment,
        stopOnDecodeError, listener);
  }

  private static GenericRow row(int value) {
    GenericRow row = new GenericRow();
    row.putValue("value", value);
    return row;
  }

  private static StreamDataDecoderResult decoded(GenericRow row) {
    return new StreamDataDecoderResult(row, null);
  }
}
