package org.apache.pinot.query.mailbox.channel;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;


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
