package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeRecordStream;
import org.hibernate.validator.constraints.NotEmpty;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutorService;

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
  public Response doBootstrap(final Payload payload) throws Exception
  {
    final StarTreeConfig config = starTreeManager.getConfig(payload.getCollection());
    if (config == null)
    {
      throw new IllegalArgumentException("No collection " + payload.getCollection());
    }

    InputStream inputStream = new URL(payload.getUri()).openStream();

    StarTreeRecordStream recordStream
            = new StarTreeRecordStream(inputStream, config.getDimensionNames(), config.getMetricNames(), "\t");

    starTreeManager.load(payload.getCollection(), recordStream);

    return Response.ok().build();
  }

  public static class Payload
  {
    @NotEmpty
    private String collection;

    @NotEmpty
    private String uri;

    @JsonProperty
    public String getCollection()
    {
      return collection;
    }

    public void setCollection(String collection)
    {
      this.collection = collection;
    }

    @JsonProperty
    public String getUri()
    {
      return uri;
    }

    public void setUri(String uri)
    {
      this.uri = uri;
    }
  }
}
