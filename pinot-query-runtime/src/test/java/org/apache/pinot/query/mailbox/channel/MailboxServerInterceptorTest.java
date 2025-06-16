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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class MailboxServerInterceptorTest {

  private static final String TEST_MAILBOX_ID = "test-mailbox-id";

  @Test
  public void testMailboxIdServerInterceptorAttachesContext() {
    ServerCall<String, String> serverCall = mock(ServerCall.class);
    MailboxServerInterceptor interceptor = new MailboxServerInterceptor();

    Metadata inboundHeaders = new Metadata();
    inboundHeaders.put(ChannelUtils.MAILBOX_ID_METADATA_KEY, TEST_MAILBOX_ID);

    // Create a dummy ServerCallHandler that captures the context value when startCall is invoked
    class DummyServerCallHandler implements ServerCallHandler<String, String> {
      String _capturedMailboxId = null;

      @Override
      public ServerCall.Listener<String> startCall(ServerCall<String, String> call, Metadata headers) {
        _capturedMailboxId = ChannelUtils.MAILBOX_ID_CTX_KEY.get();
        return new ServerCall.Listener<>() { };
      }
    }
    DummyServerCallHandler dummyHandler = new DummyServerCallHandler();

    interceptor.interceptCall(serverCall, inboundHeaders, dummyHandler);
    assertEquals(dummyHandler._capturedMailboxId, TEST_MAILBOX_ID,
      "MailboxIdServerInterceptor should attach mailboxId to Context when present");
  }

  @Test
  public void testMailboxIdServerInterceptorNoMailboxId() {
    ServerCall<String, String> serverCall = mock(ServerCall.class);
    MailboxServerInterceptor interceptor = new MailboxServerInterceptor();

    // Dummy handler that captures context (should remain null in this case)
    class DummyServerCallHandler implements ServerCallHandler<String, String> {
      final ServerCall.Listener<String> _listenerToReturn = new ServerCall.Listener<>() {
      };
      String _capturedMailboxId = "initial";

      @Override
      public ServerCall.Listener<String> startCall(ServerCall<String, String> call, Metadata headers) {
        _capturedMailboxId = ChannelUtils.MAILBOX_ID_CTX_KEY.get();
        return _listenerToReturn;
      }
    }
    DummyServerCallHandler dummyHandler = new DummyServerCallHandler();

    ServerCall.Listener<String> resultingListener = interceptor.interceptCall(serverCall, new Metadata(), dummyHandler);
    assertNull(dummyHandler._capturedMailboxId,
      "MailboxIdServerInterceptor should not set Context when mailbox-id header is absent");
    assertSame(resultingListener, dummyHandler._listenerToReturn,
      "When no mailboxId is present, interceptor should return the original listener from next");
  }
}
