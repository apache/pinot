package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeRecordStream;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URL;

@Path("/bootstrap")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeBootstrapResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeBootstrapResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @POST
  @Timed
  public Response doBootstrap(ThirdEyeBootstrapPayload payload) throws Exception
  {
    StarTreeConfig config = starTreeManager.getConfig(payload.getCollection());
    if (config == null)
    {
      throw new IllegalArgumentException("No collection " + payload.getCollection());
    }

    InputStream inputStream = new URL(payload.getUri()).openStream();
    StarTreeRecordStream recordStream
            = new StarTreeRecordStream(inputStream, config.getDimensionNames(), config.getMetricNames(), "\t");

    starTreeManager.load(payload.getCollection(), recordStream);

    return Response.status(Response.Status.ACCEPTED).build();
  }
}
