package org.apache.pinot.core.query.executor;

import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


public class MaxTasksExecutorTest {

  private static final int MAX_THREADS = 5;

  @Test
  public void testExecutor()
      throws Exception {
    MaxTasksExecutor ex = new MaxTasksExecutor(MAX_THREADS, Executors.newCachedThreadPool());

    final Semaphore sem1 = new Semaphore(0);
    final Semaphore sem2 = new Semaphore(0);

    for (int i = 1; i <= MAX_THREADS; i++) {
      ex.execute(() -> {
        sem2.release();
        sem1.acquireUninterruptibly();
      });
      sem2.acquire();
    }

    try {
      ex.execute(() -> {
      });
      fail("Should not allow more than " + MAX_THREADS + " threads");
    } catch (Exception e) {
      // as expected
    }

    for (int i = MAX_THREADS; i > 0; i--) {
      sem1.release();
    }

    try {
      ex.execute(() -> {
      });
    } catch (Exception e) {
      fail("Exception submitting task after release: " + e);
    }
  }
}
