package com.linkedin.thirdeye.dashboard.configs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.linkedin.thirdeye.api.CollectionSchema;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractConfig {

  protected String yaml;
  protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
  }

  public AbstractConfig() {

  }

  public abstract String toJSON() throws Exception;

  public abstract String getConfigName();

  public static <T extends AbstractConfig> T fromJSON(String json,
      Class<? extends AbstractConfig> configTypeClass) throws Exception {
    if (configTypeClass.getName().equals(DashboardConfig.class.getName())) {
      TypeReference<DashboardConfig> typeRef = new TypeReference<DashboardConfig>() {
      };
      T value = OBJECT_MAPPER.readValue(json, typeRef);
      return value;

    } else if (configTypeClass.getName().equals(CollectionSchema.class.getName())) {
      TypeReference<CollectionSchema> typeRef = new TypeReference<CollectionSchema>() {
      };
      T value = OBJECT_MAPPER.readValue(json, typeRef);
      return value;

    } else if (configTypeClass.getName().equals(CollectionConfig.class.getName())) {
      TypeReference<CollectionConfig> typeRef = new TypeReference<CollectionConfig>() {
      };
      T value = OBJECT_MAPPER.readValue(json, typeRef);
      return value;

    }
    return null;
  }

}
