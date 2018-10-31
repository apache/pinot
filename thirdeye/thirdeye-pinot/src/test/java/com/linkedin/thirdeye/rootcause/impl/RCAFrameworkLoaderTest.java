package com.linkedin.thirdeye.rootcause.impl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RCAFrameworkLoaderTest {
  @Test
  public void testAugmentPathProperty() {
    File rcaConfig = new File("/my/path/config.yml");

    Map<String, Object> prop = new HashMap<>();
    prop.put("key", "value");
    prop.put("absolutePath", "/absolute/path.txt");
    prop.put("relativePath", "relative_path.txt");
    prop.put("path", "another_relative_path.txt");

    Map<String, Object> aug = RCAFrameworkLoader.augmentPathProperty(prop, rcaConfig);

    Assert.assertEquals(aug.get("key"), "value");
    Assert.assertEquals(aug.get("absolutePath"), "/absolute/path.txt");
    Assert.assertEquals(aug.get("relativePath"), "/my/path/relative_path.txt");
    Assert.assertEquals(aug.get("path"), "/my/path/another_relative_path.txt");
  }
}
