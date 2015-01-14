package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.api.DimensionKey;
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

    String[] key = new String[starTree.getConfig().getDimensions().size()];

    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
    {
      DimensionSpec dimensionSpec = starTree.getConfig().getDimensions().get(i);

      List<String> dimensionValues = uriInfo.getQueryParameters().get(dimensionSpec.getName());

      if (dimensionValues == null || dimensionValues.isEmpty())
      {
        key[i] = StarTreeConstants.STAR;
      }
      else if (dimensionValues.size() == 1)
      {
        key[i] = dimensionValues.get(0);
      }
      else
      {
        key[i] = StarTreeConstants.ALL; // will filter later
      }
    }

    builder.setDimensionKey(new DimensionKey(key));

    return builder;
  }
}
