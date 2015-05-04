package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Arrays;

public class TestDimensionStoreMutableImpl {
  private StarTreeConfig config;

  @BeforeClass
  public void beforeClass() throws Exception {
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("sample-config.yml");
    config = StarTreeConfig.decode(inputStream);
  }

  @Test
  public void testAddKeys() {
    // Create
    DimensionStore dimensionStore = new DimensionStoreMutableImpl(config.getDimensions());
    Assert.assertEquals(dimensionStore.getDimensionKeyCount(), 0);

    // Add some keys (implicitly via find)
    DimensionKey key = DimensionKey.make("A1", "B1", "C1");
    dimensionStore.findMatchingKeys(key);
    Assert.assertEquals(dimensionStore.getDimensionKeys(), Arrays.asList(key));
    Assert.assertEquals(dimensionStore.getDimensionKeyCount(), 1);

    // Add some more keys (implicitly via find)
    key = DimensionKey.make("A2", "B2", "C2");
    dimensionStore.findMatchingKeys(key);
    Assert.assertEquals(dimensionStore.getDimensionKeyCount(), 2);

    // Add same key again (should not add more)
    dimensionStore.findMatchingKeys(key);
    Assert.assertEquals(dimensionStore.getDimensionKeyCount(), 2);
  }
}
