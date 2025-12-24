package org.apache.pinot.server.starter.helix;

import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * Manages the custom Helix state transition thread pools for Pinot server.
 * {@link StateTransitionThreadPoolManager#getExecutorService} may only be called once for each argument combination,
 * and the returned {@link ExecutorService} would then be cached in Helix to serve all subsequent state transition
 * messages of that argument combination. See {@link StateModelFactory#getExecutorService(Message.MessageInfo)}
 */
public interface StateTransitionThreadPoolManager {
  @Nullable
  StateModelFactory.CustomizedExecutorService getExecutorService(Message.MessageInfo messageInfo);

  @Nullable
  ExecutorService getExecutorService(String resourceName, String fromState, String toState);

  @Nullable
  ExecutorService getExecutorService(String resourceName);

  void shutdown();
}
