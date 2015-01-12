package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.api.DimensionSpec;
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

    for (DimensionSpec dimensionSpec : starTree.getConfig().getDimensions())
    {
      List<String> dimensionValues = uriInfo.getQueryParameters().get(dimensionSpec.getName());

      if (dimensionValues == null || dimensionValues.isEmpty())
      {
        builder.setDimensionValue(dimensionSpec.getName(), StarTreeConstants.STAR);
      }
      else if (dimensionValues.size() == 1)
      {
        builder.setDimensionValue(dimensionSpec.getName(), dimensionValues.get(0));
      }
      else
      {
        builder.setDimensionValue(dimensionSpec.getName(), StarTreeConstants.ALL);  // will filter later
      }
    }

    return builder;
  }
}
