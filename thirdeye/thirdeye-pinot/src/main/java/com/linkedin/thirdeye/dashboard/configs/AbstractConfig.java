package com.linkedin.thirdeye.dashboard.configs;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.linkedin.thirdeye.api.CollectionSchema;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class AbstractConfig {

  protected String yaml;
  protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
  }

  public AbstractConfig() {

  }

  public abstract String toJSON() throws Exception;

  public static <T extends AbstractConfig> T fromJSON(String json,
      Class<? extends AbstractConfig> configTypeClass) throws Exception {
    System.out.println(configTypeClass.getName()+" " +(CollectionSchema.class.getName()));
    if (configTypeClass.getName().equals(DashboardConfig.class.getName())) {
      TypeReference<DashboardConfig> typeRef = new TypeReference<DashboardConfig>() {
      };
      T value = OBJECT_MAPPER.readValue(json, typeRef);
      return value;

    } else if (configTypeClass.getName().equals(CollectionSchema.class.getName())) {
      System.out.println("hi");
      TypeReference<CollectionSchema> typeRef = new TypeReference<CollectionSchema>() {
      };
      T value = OBJECT_MAPPER.readValue(json, typeRef);
      return value;

    }
    return null;
  }

}
