package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;

import javax.ws.rs.core.UriInfo;
import java.util.List;

public class ThirdEyeUriUtils
{
  public static StarTreeQueryImpl.Builder createQueryBuilder(StarTree starTree, UriInfo uriInfo)
  {
    StarTreeQueryImpl.Builder builder = new StarTreeQueryImpl.Builder();

    for (String dimensionName : starTree.getConfig().getDimensionNames())
    {
      List<String> dimensionValues = uriInfo.getQueryParameters().get(dimensionName);

      if (dimensionValues == null || dimensionValues.isEmpty())
      {
        builder.setDimensionValue(dimensionName, StarTreeConstants.STAR);
      }
      else if (dimensionValues.size() == 1)
      {
        builder.setDimensionValue(dimensionName, dimensionValues.get(0));
      }
      else
      {
        builder.setDimensionValue(dimensionName, StarTreeConstants.ALL);  // will filter later
      }
    }

    return builder;
  }
}
