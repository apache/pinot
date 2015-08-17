package com.linkedin.thirdeye.dashboard.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.dashboard.api.DimensionGroup;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.custom.CustomDashboardSpec;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ConfigCache {
  private final String GROUPS_FILE_NAME = "groups.yml";
  private final ObjectMapper yamlObjectMapper;
  private final LoadingCache<CacheKey, CustomDashboardSpec> customDashboardCache;
  private final LoadingCache<String, DimensionGroupSpec> dimensionGroupCache;

  private File customDashboardRoot;
  private File collectionConfigRoot;

  public ConfigCache() {
    this.yamlObjectMapper = new ObjectMapper(new YAMLFactory());
    this.customDashboardCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new CustomDashboardSpecLoader());
    this.dimensionGroupCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new DimensionGroupSpecLoader());
  }

  public void setCustomDashboardRoot(File customDashboardRoot) {
    this.customDashboardRoot = customDashboardRoot;
  }

  public void setCollectionConfigRoot(File collectionConfigRoot) {
    this.collectionConfigRoot = collectionConfigRoot;
  }

  /** Returns a custom dashboard spec */
  public CustomDashboardSpec getCustomDashboardSpec(String collection, String name) throws Exception {
    CacheKey key = new CacheKey(collection, name);
    return customDashboardCache.get(key);
  }

  /** Removes custom dashboard spec from cache */
  public void invalidateCustomDashboardSpec(String collection, String name) {
    CacheKey key = new CacheKey(collection, name);
    customDashboardCache.invalidate(key);
  }

  /** Returns a dimension group spec for a collection */
  public DimensionGroupSpec getDimensionGroupSpec(String collection) throws Exception {
    if (customDashboardRoot == null) {
      return null;
    }
    DimensionGroupSpec spec = dimensionGroupCache.get(collection);
    if (spec.equals(DimensionGroupSpec.emptySpec(collection))) {
      invalidateDimensionGroupSpec(collection);
    }
    return spec;
  }

  /** Removes a dimension group spec from cache */
  public void invalidateDimensionGroupSpec(String collection) {
    dimensionGroupCache.invalidate(collection);
  }

  /** Clears all cached configs */
  public void clear() {
    customDashboardCache.invalidateAll();
    dimensionGroupCache.invalidateAll();
  }

  private class CustomDashboardSpecLoader extends CacheLoader<CacheKey, CustomDashboardSpec> {
    @Override
    public CustomDashboardSpec load(CacheKey key) throws Exception {
      File collectionDir = new File(customDashboardRoot, key.getCollection());
      File configFile = new File(collectionDir, key.getName());
      return yamlObjectMapper.readValue(configFile, CustomDashboardSpec.class);
    }
  }

  private class DimensionGroupSpecLoader extends CacheLoader<String, DimensionGroupSpec> {
    @Override
    public DimensionGroupSpec load(String key) throws Exception {
      File collectionDir = new File(collectionConfigRoot, key);
      File configFile = new File(collectionDir, GROUPS_FILE_NAME);
      if (!configFile.exists()) {
        DimensionGroupSpec emptySpec = new DimensionGroupSpec();
        emptySpec.setCollection(key);
        emptySpec.setGroups(new ArrayList<DimensionGroup>(0));
        return emptySpec;
      }
      return yamlObjectMapper.readValue(configFile, DimensionGroupSpec.class);
    }
  }

  private static class CacheKey {
    private final String collection;
    private final String name;

    CacheKey(String collection, String name) {
      this.collection = collection;
      this.name = name;
    }

    String getCollection() {
      return collection;
    }

    String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(collection, name);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey k = (CacheKey) o;
      return k.getName().equals(name) && k.getCollection().equals(collection);
    }
  }
}
