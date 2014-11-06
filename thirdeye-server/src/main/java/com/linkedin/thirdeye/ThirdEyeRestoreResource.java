package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeManager;
import org.hibernate.validator.constraints.NotEmpty;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;

@Path("/restore")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeRestoreResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeRestoreResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @POST
  @Timed
  public Response restore(final Payload payload) throws Exception
  {
    InputStream treeStream = URI.create(payload.getTreeUri()).toURL().openStream();
    InputStream configStream = URI.create(payload.getConfigUri()).toURL().openStream();
    starTreeManager.restore(payload.getCollection(), treeStream, configStream);
    return Response.ok().build();
  }

  public static class Payload
  {
    @NotEmpty
    private String collection;

    @NotEmpty
    private String treeUri;

    @NotEmpty
    private String configUri;

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
    public String getTreeUri()
    {
      return treeUri;
    }

    public void setTreeUri(String treeUri)
    {
      this.treeUri = treeUri;
    }

    @JsonProperty
    public String getConfigUri()
    {
      return configUri;
    }

    public void setConfigUri(String configUri)
    {
      this.configUri = configUri;
    }
  }
}
