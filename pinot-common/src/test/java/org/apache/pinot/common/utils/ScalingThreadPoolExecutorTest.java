package org.apache.pinot.common.utils;

import java.util.concurrent.ThreadPoolExecutor;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ScalingThreadPoolExecutorTest {

  @Test
  public void testCreateThreadPerRunnable() {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, 5, 500);
    assertEquals(executorService.getLargestPoolSize(), 0);
    for (int i = 0; i < 5; i++) {
      executorService.submit(getSleepingRunnable());
    }
    assertTrue(executorService.getLargestPoolSize() >= 2);
  }

  @Test
  public void testCreateThreadsUpToMax() {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, 5, 500);
    for (int i = 0; i < 10; i++) {
      executorService.submit(getSleepingRunnable());
    }
    assertEquals(executorService.getLargestPoolSize(), 5);
  }

  @Test
  public void testScaleDownAfterDelay() {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, 5, 500);
    for (int i = 0; i < 2; i++) {
      executorService.submit(getSleepingRunnable());
    }
    TestUtils.waitForCondition(aVoid -> executorService.getPoolSize() == 0, 2000,
        "Timed out waiting for thread pool to scale down");
  }

  private Runnable getSleepingRunnable() {
    return () -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
  }
}
