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
package org.apache.pinot.segment.local.realtime.writer;

import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;


/// Published-then-rethrown index() must not abort stateless consumption (issue #16316).
public class StatelessRealtimeSegmentWriterTest {

  @Test
  public void testPublishedRepairDoesNotAbort()
      throws Exception {
    MutableSegmentImpl segment = mock(MutableSegmentImpl.class);
    when(segment.getNumDocsIndexed()).thenReturn(0, 1);
    when(segment.canAddMore()).thenReturn(true);
    when(segment.canTakeMoreRows()).thenReturn(true);
    when(segment.index(any(), any())).thenThrow(new RuntimeException("repaired then rethrown"));
    StatelessRealtimeSegmentWriter.indexTransformedRow(segment, new GenericRow(), mock(StreamMessageMetadata.class));
  }

  @Test
  public void testUnpublishedFailureRethrows()
      throws Exception {
    MutableSegmentImpl segment = mock(MutableSegmentImpl.class);
    when(segment.getNumDocsIndexed()).thenReturn(0);
    when(segment.canAddMore()).thenReturn(true);
    when(segment.index(any(), any())).thenThrow(new RuntimeException("failed before mutation"));
    expectThrows(RuntimeException.class,
        () -> StatelessRealtimeSegmentWriter.indexTransformedRow(segment, new GenericRow(),
            mock(StreamMessageMetadata.class)));
  }

  @Test
  public void testTerminalPublishedRepairRethrows()
      throws Exception {
    MutableSegmentImpl segment = mock(MutableSegmentImpl.class);
    when(segment.getNumDocsIndexed()).thenReturn(0, 1);
    when(segment.canAddMore()).thenReturn(false);
    when(segment.canTakeMoreRows()).thenReturn(false);
    when(segment.index(any(), any())).thenThrow(new RuntimeException("unrecoverable"));
    expectThrows(RuntimeException.class,
        () -> StatelessRealtimeSegmentWriter.indexTransformedRow(segment, new GenericRow(),
            mock(StreamMessageMetadata.class)));
  }
}
