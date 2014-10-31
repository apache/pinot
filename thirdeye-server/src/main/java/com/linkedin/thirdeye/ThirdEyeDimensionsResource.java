package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/dimensions")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeDimensionsResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeDimensionsResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{collection}")
  @Timed
  public Map<String, List<String>> getDimensions(@PathParam("collection") String collection)
  {
    final StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    Map<String, List<String>> allDimensionValues = new HashMap<String, List<String>>();

    for (String dimensionName : starTree.getConfig().getDimensionNames())
    {
      List<String> dimensionValues = new ArrayList<String>(starTree.getDimensionValues(dimensionName));
      Collections.sort(dimensionValues);
      allDimensionValues.put(dimensionName, dimensionValues);
    }

    return allDimensionValues;
  }
}
