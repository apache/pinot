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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class MailboxClientInterceptorTest {

  private static final String TEST_MAILBOX_ID = "test-mailbox-id";
  private static final MethodDescriptor.Marshaller<String> STRING_MARSHALLER = new MethodDescriptor.Marshaller<>() {
    @Override
    public InputStream stream(String value) {
      return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String parse(InputStream stream) {
      try {
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };

  @Test
  public void testMailboxIdClientInterceptorAddsHeader() {
    MailboxClientInterceptor interceptor = new MailboxClientInterceptor(TEST_MAILBOX_ID);
    Channel mockChannel = Mockito.mock(Channel.class);
    ClientCall<String, String> mockCall = Mockito.mock(ClientCall.class);
    when(mockChannel.newCall(any(MethodDescriptor.class), any())).thenReturn(mockCall);

    MethodDescriptor<String, String> dummyMethod = MethodDescriptor.newBuilder(STRING_MARSHALLER, STRING_MARSHALLER)
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName("dummy/method").build();
    ClientCall<String, String> interceptedCall = interceptor.interceptCall(dummyMethod, CallOptions.DEFAULT,
      mockChannel);

    ClientCall.Listener<String> dummyListener = new ClientCall.Listener<>() {
    };
    Metadata headers = new Metadata();
    interceptedCall.start(dummyListener, headers);

    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(mockCall).start(any(ClientCall.Listener.class), metadataCaptor.capture());
    Metadata capturedHeaders = metadataCaptor.getValue();
    String sentMailboxId = capturedHeaders.get(ChannelUtils.MAILBOX_ID_METADATA_KEY);
    assertEquals(sentMailboxId, TEST_MAILBOX_ID, "MailboxIdClientInterceptor should add mailbox-id metadata");
  }
}
