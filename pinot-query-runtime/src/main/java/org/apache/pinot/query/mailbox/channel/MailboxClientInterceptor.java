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
