package com.linkedin.thirdeye.task;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import com.sun.jersey.api.NotFoundException;
import io.dropwizard.servlets.tasks.Task;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileFilter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExpireTask extends Task
{
  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final StarTreeManager manager;
  private final File rootDir;

  public ExpireTask(StarTreeManager manager, File rootDir)
  {
    super("expire");
    this.manager = manager;
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

    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No star tree for collection " + collection);
    }

    File metricStoreDir = new File(PATH_JOINER.join(
            rootDir.getAbsolutePath(), collection, StarTreeConstants.DATA_DIR_NAME, StarTreeConstants.METRIC_STORE));

    File[] metricIndexFiles = metricStoreDir.listFiles(new FileFilter()
    {
      @Override
      public boolean accept(File pathname)
      {
        return pathname.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
      }
    });

    long retentionPeriod
            = TimeUnit.MILLISECONDS.convert(starTree.getConfig().getTime().getRetention().getSize(),
                                            starTree.getConfig().getTime().getRetention().getUnit());

    long oldestValidTime = System.currentTimeMillis() - retentionPeriod;

    if (metricIndexFiles != null)
    {
      for (File metricIndexFile : metricIndexFiles)
      {
        List<MetricIndexEntry> indexEntries = StorageUtils.readMetricIndex(metricIndexFile);

        boolean expired = true;

        Long minTime = null;
        Long maxTime = null;

        for (MetricIndexEntry indexEntry : indexEntries)
        {
          long startTimeMillis
                  = TimeUnit.MILLISECONDS.convert(indexEntry.getTimeRange().getStart(),
                                                  starTree.getConfig().getTime().getBucket().getUnit());

          long endTimeMillis
                  = TimeUnit.MILLISECONDS.convert(indexEntry.getTimeRange().getEnd(),
                                                  starTree.getConfig().getTime().getBucket().getUnit());

          if (startTimeMillis >= 0 && (minTime == null || startTimeMillis < minTime))
          {
            minTime = startTimeMillis;
          }

          if (endTimeMillis >= 0 && (maxTime == null || endTimeMillis > maxTime))
          {
            maxTime = endTimeMillis;
          }

          if (endTimeMillis >= oldestValidTime)
          {
            expired = false;
            break;
          }
        }

        // Only delete file if all time ranges in index entry are expired
        if (expired)
        {
          FileUtils.forceDelete(metricIndexFile);

          FileUtils.forceDelete(getBufferFile(metricIndexFile));

          printWriter.print("Expired metric file ID " + getFileId(metricIndexFile));

          if (minTime != null && maxTime != null)
          {
            printWriter.print("(");
            printWriter.print(new DateTime(minTime, DateTimeZone.UTC));
            printWriter.print(", ");
            printWriter.print(new DateTime(maxTime, DateTimeZone.UTC));
            printWriter.print(")");
          }

          printWriter.println();
          printWriter.flush();
        }
      }
    }
  }

  private static File getBufferFile(File metricIndexFile)
  {
    return new File(metricIndexFile.getParent(), getFileId(metricIndexFile) + StarTreeConstants.BUFFER_FILE_SUFFIX);
  }

  private static String getFileId(File metricIndexFile)
  {
    return metricIndexFile.getName()
                          .substring(0, metricIndexFile.getName()
                                                       .lastIndexOf(StarTreeConstants.INDEX_FILE_SUFFIX));
  }
}
