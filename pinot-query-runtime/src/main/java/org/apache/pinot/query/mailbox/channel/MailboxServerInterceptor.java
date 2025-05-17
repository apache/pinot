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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;


/**
 * MailboxServerInterceptor is used in GrpcMailboxServer to intercept mailboxId from headers
 * to inject into context.
 */
public class MailboxServerInterceptor implements ServerInterceptor {
  @Override
  public <T, R> ServerCall.Listener<T> interceptCall(
    ServerCall<T, R> call, Metadata headers, ServerCallHandler<T, R> next) {
    String mailboxId = headers.get(ChannelUtils.MAILBOX_ID_METADATA_KEY);
    if (mailboxId != null) {
      Context context = Context.current().withValue(ChannelUtils.MAILBOX_ID_CTX_KEY, mailboxId);
      return Contexts.interceptCall(context, call, headers, next);
    }
    return next.startCall(call, headers);
  }
}
