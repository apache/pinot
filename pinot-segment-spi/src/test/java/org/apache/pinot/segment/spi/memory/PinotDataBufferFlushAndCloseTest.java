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
package org.apache.pinot.segment.spi.memory;

import java.io.IOException;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class PinotDataBufferFlushAndCloseTest {

  @Test
  public void testFlushAndCloseAggregatesFailures()
      throws IOException {
    PinotDataBuffer buffer1 = mock(PinotDataBuffer.class);
    PinotDataBuffer buffer2 = mock(PinotDataBuffer.class);
    doThrow(new RuntimeException("flush failed")).when(buffer1).close();
    doThrow(new IOException("close failed")).when(buffer2).close();

    try {
      PinotDataBuffer.flushAndClose(buffer1, buffer2);
      fail("Expected IOException");
    } catch (IOException e) {
      assertEquals(e.getCause().getMessage(), "flush failed");
      assertEquals(e.getCause().getSuppressed().length, 1);
      assertEquals(e.getCause().getSuppressed()[0].getMessage(), "close failed");
    }

    // Every close is attempted, so a failure in one buffer does not leak the remaining buffers.
    verify(buffer1).close();
    verify(buffer2).close();
  }

  @Test
  public void testFlushAndCloseHappyPath()
      throws IOException {
    PinotDataBuffer buffer = mock(PinotDataBuffer.class);
    PinotDataBuffer.flushAndClose(buffer);
    verify(buffer).close();
  }

  @Test
  public void testFlushAndCloseSkipsNulls()
      throws IOException {
    PinotDataBuffer buffer = mock(PinotDataBuffer.class);
    PinotDataBuffer.flushAndClose(null, buffer, null);
    verify(buffer).close();
  }
}
