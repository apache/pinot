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
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;


public class MailboxClientInterceptor implements ClientInterceptor {
  private final String _mailboxId;

  public MailboxClientInterceptor(String mailboxId) {
    _mailboxId = mailboxId;
  }

  @Override
  public <T, R> ClientCall<T, R> interceptCall(
    MethodDescriptor<T, R> method, CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<R> responseListener, Metadata headers) {
        headers.put(ChannelUtils.MAILBOX_ID_METADATA_KEY, _mailboxId);
        super.start(responseListener, headers);
      }
    };
  }
}
