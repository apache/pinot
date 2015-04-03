package com.linkedin.thirdeye.healthcheck;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

public class TimeRangeContiguityHealthCheck extends HealthCheck{

  public static final String NAME = "timRangeContiguityHealthCheck";

  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final File rootDir;
  private final StarTreeManager manager;

  public TimeRangeContiguityHealthCheck(File rootDir, StarTreeManager manager)
  {
    this.rootDir = rootDir;
    this.manager = manager;
  }

  @Override
  protected Result check() throws Exception
  {
    for (String collection : manager.getCollections())
    {
      // Get all timeranges present in the indexes
      List<TimeRange> indexTimeRanges = new ArrayList<TimeRange>();
      List<TimeRange> missingRanges = new ArrayList<TimeRange>();

      File metricStoreDir = new File(PATH_JOINER.join(
          rootDir, collection, StarTreeConstants.DATA_DIR_NAME, StarTreeConstants.METRIC_STORE));
      File[] metricIndexFiles = metricStoreDir.listFiles(INDEX_FILE_FILTER);

      if (metricIndexFiles != null)
      {
        for (File metricIndexFile : metricIndexFiles)
        {
          List<MetricIndexEntry> indexEntries = StorageUtils.readMetricIndex(metricIndexFile);

          for (MetricIndexEntry metricIndexEntry : indexEntries)
          {
            if (!indexTimeRanges.contains(metricIndexEntry.getTimeRange()))
            {
              indexTimeRanges.add(metricIndexEntry.getTimeRange());
            }
          }
        }
      }


      //check that timeranges are contiguous
      if (indexTimeRanges.size() != 0)
      {
        Collections.sort(indexTimeRanges);

        long minStart = indexTimeRanges.get(0).getStart();
        long maxEnd = indexTimeRanges.get(0).getEnd();

        for (TimeRange timerange : indexTimeRanges)
        {

          if (timerange.getStart() <= maxEnd + 1)
          {
            maxEnd = (timerange.getEnd() > maxEnd) ? timerange.getEnd() : maxEnd ;
          }
          else
          {
            missingRanges.add(new TimeRange(maxEnd + 1, timerange.getStart() - 1));
            minStart = timerange.getStart();
            maxEnd = timerange.getEnd();
          }

        }

        if (missingRanges.size() != 0)
        {
          String listMissingRanges = "";
          for (TimeRange missingRange : missingRanges)
          {
            listMissingRanges = listMissingRanges + "["+missingRange.getStart()+":"+missingRange.getEnd()+"] ";
          }
          throw new IllegalStateException("Collection "+collection+" is missing timeranges "+listMissingRanges);
        }

      }
    }

    return Result.healthy();
  }

  private static final FileFilter INDEX_FILE_FILTER = new FileFilter()
  {
    @Override
    public boolean accept(File pathname)
    {
      return pathname.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
    }
  };

}
