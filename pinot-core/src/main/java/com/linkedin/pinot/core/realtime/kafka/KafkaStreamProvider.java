package com.linkedin.pinot.core.realtime.kafka;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;

public class KafkaStreamProvider implements StreamProvider{

  @Override
  public void init(StreamProviderConfig streamProviderConfig) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setOffset(long offset) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GenericRow next() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GenericRow next(long offset) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long currentOffset() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void commit() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void commit(long offset) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub
    
  }

}
