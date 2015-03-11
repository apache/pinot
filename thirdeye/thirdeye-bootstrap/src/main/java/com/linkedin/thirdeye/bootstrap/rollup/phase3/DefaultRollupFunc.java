package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import java.util.Map;
import java.util.Map.Entry;

import com.linkedin.thirdeye.api.RollupSelectFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.RollupThresholdFunction;

/**
 * Default implementation that selects the one rolls up minimum number of
 * dimensions and clears the threshold
 *
 * @author kgopalak
 *
 */
public class DefaultRollupFunc implements RollupSelectFunction
{
  private static final Logger LOG = LoggerFactory
      .getLogger(DefaultRollupFunc.class);
  @Override
  public DimensionKey rollup(DimensionKey rawDimensionKey,
      Map<DimensionKey, MetricTimeSeries> possibleRollups,
      RollupThresholdFunction func) {
    int minCount = rawDimensionKey.getDimensionValues().length + 1 ;
    DimensionKey selectedRollup = null;
    LOG.info("Start find roll up for {}", rawDimensionKey);
    for (Entry<DimensionKey, MetricTimeSeries> entry : possibleRollups
        .entrySet()) {
      DimensionKey key = entry.getKey();
      LOG.info("Trying {}", key);
      String[] dimensionsValues = key.getDimensionValues();
      if (func.isAboveThreshold(entry.getValue())) {
        LOG.debug("passed threshold");
        int count = 0;
        for (String val : dimensionsValues) {
          if ("?".equalsIgnoreCase(val)) {
            count += 1;
          }
        }
        LOG.info("count:{} mincount:{}", count, minCount);
        if (count < minCount) {
          minCount = count;
          selectedRollup = key;
          LOG.info("setting selectedrollup:{}", selectedRollup);
        }
      }
    }
    if(selectedRollup ==null){
      StringBuilder sb = new StringBuilder();
      for (Entry<DimensionKey, MetricTimeSeries> entry : possibleRollups
          .entrySet()) {
        sb.append(entry.getKey());
        sb.append("=");
        sb.append(entry.getValue());
        sb.append("\n");
      }
      LOG.error("cannot find roll up for {} possiblerollups:{}",rawDimensionKey, sb.toString() );
    }
    return selectedRollup;
  }

}
