package org.apache.pinot.query.mailbox.channel;

import io.grpc.stub.ServerCallStreamObserver;
import org.apache.pinot.common.proto.Mailbox;


class OnReadyHandler implements Runnable {
  private boolean _wasReady = false;
  private ServerCallStreamObserver<Mailbox.MailboxStatus> _serverCallStreamObserver;
  public OnReadyHandler(ServerCallStreamObserver<Mailbox.MailboxStatus> serverCallStreamObserver){
    _serverCallStreamObserver = serverCallStreamObserver;
  }

  public void setReady(boolean wasReady){
    _wasReady = wasReady;
  }

  @Override
  public void run(){
    if(_serverCallStreamObserver.isReady() && !_wasReady){
      _wasReady = true;
      _serverCallStreamObserver.request(1);
    }
  }
}
