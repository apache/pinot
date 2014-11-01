package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeCollectionsResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeCollectionsResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Timed
  public List<String> getCollections()
  {
    List<String> collections = new ArrayList<String>(starTreeManager.getCollections());
    Collections.sort(collections);
    return collections;
  }
}
