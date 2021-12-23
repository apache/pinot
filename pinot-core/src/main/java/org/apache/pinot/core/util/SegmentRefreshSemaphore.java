package org.apache.pinot.core.util;

import java.util.concurrent.Semaphore;
import org.slf4j.Logger;


/**
 * Wrapper class for semaphore used to control concurrent segment reload/refresh
 */
public class SegmentRefreshSemaphore {

  private final Semaphore _semaphore;

  public SegmentRefreshSemaphore(int permits, boolean fair) {
    if (permits > 0) {
      _semaphore = new Semaphore(permits, fair);
    } else {
      _semaphore = null;
    }
  }

  public void acquireSema(String context, Logger logger)
      throws InterruptedException {
    if (_semaphore != null) {
      long startTime = System.currentTimeMillis();
      logger.info("Waiting for lock to refresh : {}, queue-length: {}", context,
          _semaphore.getQueueLength());
      _semaphore.acquire();
      logger.info("Acquired lock to refresh segment: {} (lock-time={}ms, queue-length={})", context,
          System.currentTimeMillis() - startTime, _semaphore.getQueueLength());
    } else {
      logger.info("Locking of refresh threads disabled (segment: {})", context);
    }
  }

  public void releaseSema() {
    if (_semaphore != null) {
      _semaphore.release();
    }
  }
}
