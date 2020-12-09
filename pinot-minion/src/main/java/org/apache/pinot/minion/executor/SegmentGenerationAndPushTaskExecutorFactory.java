package org.apache.pinot.minion.executor;

public class SegmentGenerationAndPushTaskExecutorFactory implements PinotTaskExecutorFactory {
  @Override
  public PinotTaskExecutor create() {
    return new SegmentGenerationAndPushTaskExecutor();
  }
}
