package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

@Path("/configs")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeConfigResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeConfigResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @POST
  @Timed
  public Response registerConfig(ThirdEyeConfigPayload payload) throws Exception
  {
    StarTreeRecordThresholdFunction thresholdFunction = null;
    if (payload.getThresholdFunctionClass() != null)
    {
      Properties props = new Properties();
      if (payload.getThresholdFunctionConfig() != null)
      {
        for (Map.Entry<String, String> entry : payload.getThresholdFunctionConfig().entrySet())
        {
          props.setProperty(entry.getKey(), entry.getValue());
        }
      }
      thresholdFunction = (StarTreeRecordThresholdFunction) Class.forName(payload.getThresholdFunctionClass()).newInstance();
      thresholdFunction.init(props);
    }

    starTreeManager.registerRecordStoreFactory(
            payload.getCollection(),
            payload.getDimensionNames(),
            payload.getMetricNames(),
            URI.create(payload.getRootUri()));

    StarTreeRecordStoreFactory recordStoreFactory = starTreeManager.getRecordStoreFactory(payload.getCollection());
    if (recordStoreFactory == null)
    {
      throw new IllegalArgumentException("No record store has been registered for collection " + payload.getCollection());
    }

    StarTreeConfig config = new StarTreeConfig.Builder()
            .setThresholdFunction(thresholdFunction)
            .setMaxRecordStoreEntries(payload.getMaxRecordStoreEntries())
            .setRecordStoreFactory(recordStoreFactory)
            .setDimensionNames(payload.getDimensionNames())
            .setMetricNames(payload.getMetricNames())
            .build();

    starTreeManager.registerConfig(payload.getCollection(), config);

    return Response.ok().build();
  }
}
