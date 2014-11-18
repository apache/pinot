package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public Map<String, List<String>> getDimensions(@PathParam("collection") String collection,
                                                 @QueryParam("rollup") boolean rollup,
                                                 @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    // Get fixed dimensions from query string
    Map<String, String> fixedDimensions = new HashMap<String, String>();
    for (String dimensionName : starTree.getConfig().getDimensionNames())
    {
      String dimensionValue = uriInfo.getQueryParameters().getFirst(dimensionName);
      if (dimensionValue != null)
      {
        dimensionValue = StarTreeConstants.STAR;
      }
      fixedDimensions.put(dimensionName, dimensionValue);
    }

    // Compute all dimension values for each dimension with those values fixed
    Map<String, List<String>> allDimensionValues = new HashMap<String, List<String>>();
    for (String dimensionName : starTree.getConfig().getDimensionNames())
    {
      Set<String> dimensionValues = starTree.getDimensionValues(dimensionName, fixedDimensions);
      allDimensionValues.put(dimensionName, new ArrayList<String>(dimensionValues));
      Collections.sort(allDimensionValues.get(dimensionName));
    }

    return allDimensionValues;
  }
}
