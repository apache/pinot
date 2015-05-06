package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class AnomalyResultHandlerLoggerImpl implements AnomalyResultHandler
{
  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyResultHandlerLoggerImpl.class);

  @Override
  public void init(StarTreeConfig starTreeConfig, Properties handlerConfig)
  {
    // Do nothing
  }

  @Override
  public void handle(DimensionKey dimensionKey, AnomalyResult result) throws IOException
  {
    if (result.isAnomaly())
    {
      if (result.getProperties() == null)
      {
        LOGGER.warn("{} is anomaly", dimensionKey);
      }
      else
      {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Object, Object> entry : result.getProperties().entrySet())
        {
          sb.append("! ").append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        sb.setLength(sb.length() - 1); // remove last new line

        LOGGER.warn("{} is anomaly\n{}", dimensionKey, sb.toString());
      }
    }
  }
}
