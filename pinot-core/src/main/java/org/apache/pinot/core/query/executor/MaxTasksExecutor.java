package org.apache.pinot.core.query.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.executor.DecoratorExecutorService;

/**
 * An Executor that allows a maximum of tasks running at the same time, rejecting immediately any excess.
 */
public class MaxTasksExecutor extends DecoratorExecutorService {

  private final AtomicInteger _running;
  private final int _max;

  public MaxTasksExecutor(int max, ExecutorService executorService) {
    super(executorService);
    _running = new AtomicInteger(0);
    _max = max;
  }

  protected void checkTaskAllowed() {
    if (_running.get() >= _max) {
      throw new IllegalStateException("Exceeded maximum number of tasks");
    }
  }

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      try {
        _running.getAndIncrement();
        return task.call();
      } finally {
        _running.decrementAndGet();
      }
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      try {
        _running.getAndIncrement();
        task.run();
      } finally {
        _running.decrementAndGet();
      }
    };
  }
}
