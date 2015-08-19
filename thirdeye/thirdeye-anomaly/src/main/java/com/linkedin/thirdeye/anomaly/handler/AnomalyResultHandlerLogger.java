package com.linkedin.thirdeye.anomaly.handler;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.HandlerProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;

public class AnomalyResultHandlerLogger implements AnomalyResultHandler
{
  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyResultHandlerLogger.class);

  @Override
  public void init(StarTreeConfig starTreeConfig, HandlerProperties handlerConfig)
  {
    // Do nothing
  }

  /**
   * Naively log anomalies
   */
  @Override
  public void handle(AnomalyDetectionTaskInfo taskInfo, DimensionKey dimensionKey, double dimensionKeyContribution,
      Set<String> metrics, AnomalyResult result) throws IOException
  {
    if (result.isAnomaly())
    {
      DateTime resultDateTime = new DateTime(result.getTimeWindow(), DateTimeZone.UTC);
      if (result.getProperties() == null || result.getProperties().isEmpty())
      {
        LOGGER.warn("{} : {} : {} is anomaly", resultDateTime, dimensionKey, result.getAnomalyScore());
      }
      else
      {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Object, Object> entry : result.getProperties().entrySet())
        {
          sb.append("! ").append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        sb.setLength(sb.length() - 1); // remove last new line

        LOGGER.warn("{} : {} : {} is anomaly\n{}", resultDateTime, dimensionKey, result.getAnomalyScore(),
            sb.toString());
      }
    }
  }

}
