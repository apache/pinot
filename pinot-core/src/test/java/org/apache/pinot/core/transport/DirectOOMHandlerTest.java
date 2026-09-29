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
package org.apache.pinot.core.transport;

import io.netty.channel.ChannelHandlerContext;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class DirectOOMHandlerTest {

  private DirectOOMHandler createHandler() {
    return new DirectOOMHandler(null, null, null, null, null);
  }

  @Test
  public void testCatchesNettyDirectMemoryOom() {
    DirectOOMHandler handler = createHandler();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    handler.exceptionCaught(ctx,
        new OutOfMemoryError("failed to allocate 310378496 byte(s) of direct memory"));

    verify(ctx, never()).fireExceptionCaught(any());
  }

  @Test
  public void testCatchesJvmDirectBufferOom() {
    DirectOOMHandler handler = createHandler();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    handler.exceptionCaught(ctx, new OutOfMemoryError("Direct buffer memory"));

    verify(ctx, never()).fireExceptionCaught(any());
  }

  @Test
  public void testPropagatesNonOomException() {
    DirectOOMHandler handler = createHandler();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    RuntimeException cause = new RuntimeException("some error");
    handler.exceptionCaught(ctx, cause);

    verify(ctx).fireExceptionCaught(cause);
  }
}
