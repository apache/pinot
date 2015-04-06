package com.linkedin.thirdeye.task;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import io.dropwizard.servlets.tasks.Task;

import java.io.File;
import java.io.FileFilter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;

public class ViewMetricIndexTask extends Task
{
  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final File rootDir;

  public ViewMetricIndexTask(File rootDir)
  {
    super("viewMetricIndex");
    this.rootDir = rootDir;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    String collection = collectionParam.iterator().next();

    File metricStoreDir = new File(PATH_JOINER.join(
            rootDir.getAbsolutePath(), collection, StarTreeConstants.DATA_DIR_PREFIX, StarTreeConstants.METRIC_STORE));

    File[] metricIndexFiles = metricStoreDir.listFiles(new FileFilter()
    {
      @Override
      public boolean accept(File pathname)
      {
        return pathname.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
      }
    });


    if (metricIndexFiles != null)
    {
      for (File metricIndexFile : metricIndexFiles)
      {
        List<MetricIndexEntry> indexEntries = StorageUtils.readMetricIndex(metricIndexFile);

        printSeparator(metricIndexFile, printWriter);
        printWriter.println(metricIndexFile);
        printSeparator(metricIndexFile, printWriter);
        printWriter.println();

        for (MetricIndexEntry indexEntry : indexEntries)
        {
          printWriter.println(indexEntry);
        }

        printWriter.println();
        printWriter.flush();
      }
    }
  }

  private static void printSeparator(File fileName, PrintWriter printWriter)
  {
    for (int i = 0; i < fileName.getAbsolutePath().length(); i++)
    {
      printWriter.print("-");
    }
    printWriter.println();
  }
}
