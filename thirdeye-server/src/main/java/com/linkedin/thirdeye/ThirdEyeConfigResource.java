package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Path("/configs")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeConfigResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeConfigResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{collection}")
  @Timed
  public Map<String, List<String>> getConfig(@PathParam("collection") String collection)
  {
    final StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    Map<String, List<String>> result = new HashMap<String, List<String>>();
    result.put("dimensionNames", starTree.getConfig().getDimensionNames());
    result.put("metricNames", starTree.getConfig().getMetricNames());
    return result;
  }


  @POST
  @Timed
  public Response registerConfig(Payload payload) throws Exception
  {
    StarTreeConfig.Builder config = new StarTreeConfig.Builder()
            .setMaxRecordStoreEntries(payload.getMaxRecordStoreEntries())
            .setDimensionNames(payload.getDimensionNames())
            .setMetricNames(payload.getMetricNames());

    if (payload.getThresholdFunctionClass() != null)
    {
      config.setThresholdFunctionClass(payload.getThresholdFunctionClass());

      if (payload.getThresholdFunctionConfig() != null)
      {
        Properties props = new Properties();
        props.putAll(payload.getThresholdFunctionConfig());
        config.setThresholdFunctionConfig(props);
      }
    }

    if (payload.getRecordStoreFactoryClass() != null)
    {
      config.setRecordStoreFactoryClass(payload.getRecordStoreFactoryClass());

      if (payload.getRecordStoreFactoryConfig() != null)
      {
        Properties props = new Properties();
        props.putAll(payload.getRecordStoreFactoryConfig());
        config.setRecordStoreFactoryConfig(props);
      }
    }

    starTreeManager.registerConfig(payload.getCollection(), config.build());

    return Response.ok().build();
  }

  public static class Payload
  {
    @NotEmpty
    private String collection;

    @NotNull
    private List<String> dimensionNames;

    @NotNull
    private List<String> metricNames;

    private Integer maxRecordStoreEntries = 10000;

    private String thresholdFunctionClass;

    private Map<String, String> thresholdFunctionConfig;

    private String recordStoreFactoryClass;

    private Map<String, String> recordStoreFactoryConfig;

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
    public List<String> getDimensionNames()
    {
      return dimensionNames;
    }

    public void setDimensionNames(List<String> dimensionNames)
    {
      this.dimensionNames = dimensionNames;
    }

    @JsonProperty
    public List<String> getMetricNames()
    {
      return metricNames;
    }

    public void setMetricNames(List<String> metricNames)
    {
      this.metricNames = metricNames;
    }

    @JsonProperty
    public Integer getMaxRecordStoreEntries()
    {
      return maxRecordStoreEntries;
    }

    public void setMaxRecordStoreEntries(Integer maxRecordStoreEntries)
    {
      this.maxRecordStoreEntries = maxRecordStoreEntries;
    }

    @JsonProperty
    public String getThresholdFunctionClass()
    {
      return thresholdFunctionClass;
    }

    public void setThresholdFunctionClass(String thresholdFunctionClass)
    {
      this.thresholdFunctionClass = thresholdFunctionClass;
    }

    @JsonProperty
    public Map<String, String> getThresholdFunctionConfig()
    {
      return thresholdFunctionConfig;
    }

    public void setThresholdFunctionConfig(Map<String, String> thresholdFunctionConfig)
    {
      this.thresholdFunctionConfig = thresholdFunctionConfig;
    }

    @JsonProperty
    public String getRecordStoreFactoryClass()
    {
      return recordStoreFactoryClass;
    }

    public void setRecordStoreFactoryClass(String recordStoreFactoryClass)
    {
      this.recordStoreFactoryClass = recordStoreFactoryClass;
    }

    @JsonProperty
    public Map<String, String> getRecordStoreFactoryConfig()
    {
      return recordStoreFactoryConfig;
    }

    public void setRecordStoreFactoryConfig(Map<String, String> recordStoreFactoryConfig)
    {
      this.recordStoreFactoryConfig = recordStoreFactoryConfig;
    }
  }
}
