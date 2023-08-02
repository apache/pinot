package org.apache.pinot.query.runtime.executor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorServiceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceUtils.class);

  private ExecutorServiceUtils() {
  }

  public static ExecutorService createDefault(String baseName) {
    return Executors.newCachedThreadPool(new NamedThreadFactory(baseName));
  }

  public static ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
    //TODO: make this configurable
    return Executors.newCachedThreadPool(new NamedThreadFactory(baseName));
  }

  public static void close(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        List<Runnable> runnables = executorService.shutdownNow();
        LOGGER.warn("Around " + runnables.size() + " didn't finish in time after a shutdown");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
