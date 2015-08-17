package com.linkedin.thirdeye.dashboard.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.dashboard.util.ConfigCache;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;

public class ClearCachesTask extends Task {
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ConfigCache configCache;

  public ClearCachesTask(DataCache dataCache, QueryCache queryCache, ConfigCache configCache) {
    super("clearCaches");
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.configCache = configCache;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    if (!params.get("skipDataCache").isEmpty()) {
      printWriter.println("Clearing data cache...");
      printWriter.flush();
      dataCache.clear();
    }

    if (!params.get("skipQueryCache").isEmpty()) {
      printWriter.println("Clearing query cache...");
      printWriter.flush();
      queryCache.clear();
    }

    if (!params.get("skipConfigCache").isEmpty()) {
      printWriter.println("Clearing config cache...");
      printWriter.flush();
      configCache.clear();
    }

    printWriter.println("Done!");
    printWriter.flush();
  }
}
