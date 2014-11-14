package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeCollectionsResource
{
  private final StarTreeManager manager;

  public ThirdEyeCollectionsResource(StarTreeManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Timed
  public List<String> getCollections()
  {
    List<String> collections = new ArrayList<String>(manager.getCollections());
    Collections.sort(collections);
    return collections;
  }

  @GET
  @Path("/{collection}")
  @Timed
  public StarTreeStats getStats(@PathParam("collection") String collection)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No tree for collection " + collection);
    }

    return starTree.getStats();
  }
}
