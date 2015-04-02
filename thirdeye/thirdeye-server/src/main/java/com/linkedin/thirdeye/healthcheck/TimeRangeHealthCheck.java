package com.linkedin.thirdeye.healthcheck;

import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.StarTreeCallback;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;


public class TimeRangeHealthCheck extends HealthCheck {

  public static final String NAME = "timeRangeHealthCheck";

  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final File rootDir;
  private final StarTreeManager manager;

  public TimeRangeHealthCheck(File rootDir, StarTreeManager manager)
  {
    this.rootDir = rootDir;
    this.manager = manager;
  }

  @Override
  protected Result check() throws Exception {

    for (String collection : manager.getCollections())
    {

      final Map<UUID, Map<TimeRange, Integer>> nodeToTimeRangeMap = new HashMap<UUID, Map<TimeRange, Integer>>();

      File metricStoreDir = new File(PATH_JOINER.join(
          rootDir, collection, StarTreeConstants.DATA_DIR_NAME, StarTreeConstants.METRIC_STORE));
      File[] metricIndexFiles = metricStoreDir.listFiles(INDEX_FILE_FILTER);

      //Create mapping of node id to number of times each timerange appears on that node, from the index files

      if (metricIndexFiles != null)
      {
        for (File metricIndexFile : metricIndexFiles)
        {

          List<MetricIndexEntry> indexEntries = StorageUtils.readMetricIndex(metricIndexFile);

          for (MetricIndexEntry metricIndexEntry : indexEntries)
          {

             UUID nodeId = metricIndexEntry.getNodeId();
             TimeRange timeRange = metricIndexEntry.getTimeRange();

             Map<TimeRange, Integer> timeRangeToCountMap = nodeToTimeRangeMap.get(nodeId);

             if (timeRangeToCountMap == null)
             {
               timeRangeToCountMap = new HashMap<TimeRange, Integer>();
               timeRangeToCountMap.put(timeRange, 1);
               nodeToTimeRangeMap.put(nodeId, timeRangeToCountMap);
             }
             else
             {
               Integer count = timeRangeToCountMap.get(timeRange);
               if (count == null)
               {
                 timeRangeToCountMap.put(timeRange, 1);
               }
               else
               {
                 timeRangeToCountMap.put(timeRange, count + 1);
               }
             }

          }
        }
      }


     // Traverse tree structure and ensure for each node, the time ranges loaded into the metric store are the same as those in the index
      manager.getStarTree(collection).eachLeaf(new StarTreeCallback()
      {

        @Override
        public void call(StarTreeNode leafNode) {

          Map<TimeRange, Integer> indexTimeRangeToCount = nodeToTimeRangeMap.get(leafNode.getId());

          if (indexTimeRangeToCount == null)
          {
            if (leafNode.getRecordStore().getTimeRangeCount().size() != 0)
            {
              throw new IllegalStateException("Found node "+ leafNode.getId()+
                  " which has no metric segments on disk but has metric segments loaded in memory");
            }
          }
          else
          {
            Map<TimeRange, Integer> nodeTimeRangeToCount = leafNode.getRecordStore().getTimeRangeCount();

            if (indexTimeRangeToCount.size() != nodeTimeRangeToCount.size())
            {
              throw new IllegalStateException("Number of timeranges in metric store are not same as index for node "
                  +leafNode.getId());
            }

            for (Map.Entry<TimeRange, Integer> entry : nodeTimeRangeToCount.entrySet())
            {
              TimeRange nodeTimeRange = entry.getKey();
              Integer nodeCount = entry.getValue();

              Integer indexCount = indexTimeRangeToCount.get(nodeTimeRange);
              if (indexCount == null)
              {
                throw new IllegalStateException("Timerange "+nodeTimeRange.toString()
                    + "exists in metric store but not in index for node "+leafNode.getId());
              }
              if (indexCount != nodeCount)
              {
                throw new IllegalStateException("Timerange "+nodeTimeRange.toString()+" appears "+nodeCount
                    + " times in metric store but "+indexCount+" times in index, for node "+leafNode.getId());
              }
            }
          }
        }
      });
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
