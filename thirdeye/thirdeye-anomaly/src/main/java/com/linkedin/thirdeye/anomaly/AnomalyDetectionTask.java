package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AnomalyDetectionTask implements Runnable
{
  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionTask.class);

  private final StarTree starTree;
  private final AnomalyDetectionFunction function;
  private final AnomalyResultHandler handler;
  private final Mode mode;

  public enum Mode
  {
    LEAF_PREFIX,
    LEAF_RECORDS
  }

  public AnomalyDetectionTask(StarTree starTree,
                              AnomalyDetectionFunction function,
                              AnomalyResultHandler handler,
                              Mode mode)
  {
    this.starTree = starTree;
    this.function = function;
    this.handler = handler;
    this.mode = mode;
  }

  @Override
  public void run()
  {
    searchAndRun(starTree.getRoot());
  }

  private void searchAndRun(StarTreeNode node)
  {
    if (node.isLeaf())
    {
      List<DimensionSpec> dimensions = starTree.getConfig().getDimensions();

      // Get all combinations we want to look at
      Set<DimensionKey> combinations;
      switch (mode)
      {
        case LEAF_PREFIX:
          DimensionKey prefixCombination = new DimensionKey(new String[dimensions.size()]);
          for (int i = 0; i < dimensions.size(); i++)
          {
            String dimensionName = dimensions.get(i).getName();

            if (node.getAncestorDimensionValues().containsKey(dimensionName)) // fixed
            {
              prefixCombination.getDimensionValues()[i] = node.getAncestorDimensionValues().get(dimensionName);
            }
            else if (node.getDimensionName().equals(dimensionName)) // fixed
            {
              prefixCombination.getDimensionValues()[i] = node.getDimensionValue();
            }
            else
            {
              prefixCombination.getDimensionValues()[i] = StarTreeConstants.STAR;
            }
          }
          combinations = Collections.singleton(prefixCombination);
          break;
        case LEAF_RECORDS:
        default:
          combinations = new HashSet<DimensionKey>();
          for (StarTreeRecord record : node.getRecordStore())
          {
            combinations.add(record.getDimensionKey());
          }
          break;
      }

      // Get time range
      TimeGranularity window = function.getWindowTimeGranularity();
      Long maxTime = node.getRecordStore().getMaxTime();
      Long minTime = window == null
              ? node.getRecordStore().getMinTime()
              : maxTime - starTree.getConfig().getTime().getBucket().getUnit().convert(window.getSize(), window.getUnit());

      // Run for all targeted combinations
      for (DimensionKey combination : combinations)
      {
        // Get that time series
        MetricTimeSeries timeSeries = starTree.getTimeSeries(
                new StarTreeQueryImpl(starTree.getConfig(), combination, new TimeRange(minTime, maxTime)));

        // Run function
        AnomalyResult result = function.analyze(combination, timeSeries);

        // Handle result
        try
        {
          handler.handle(combination, result);
        }
        catch (IOException e)
        {
          LOGGER.error("Exception in handing result for {}: {}", combination, e);
        }
      }
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        searchAndRun(child);
      }
      searchAndRun(node.getOtherNode());
      searchAndRun(node.getStarNode());
    }
  }
}
