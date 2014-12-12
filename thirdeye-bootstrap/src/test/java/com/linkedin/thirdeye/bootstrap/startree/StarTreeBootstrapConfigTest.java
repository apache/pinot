package com.linkedin.thirdeye.bootstrap.startree;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeConfig;

public class StarTreeBootstrapConfigTest {
  @Test
  public void testSimple() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    URL configResource = ClassLoader.getSystemResource("star_tree_bootstrap_config.json");
    System.out.println(configResource);
    InputStream resourceAsStream = ClassLoader
        .getSystemResourceAsStream("star_tree_bootstrap_config.json");
    System.out.println(IOUtils.readLines(resourceAsStream));

    resourceAsStream = ClassLoader
        .getSystemResourceAsStream("star_tree_bootstrap_config.json");
    System.out.println("resourceAsStream:" + resourceAsStream);
    JsonNode jsonNode = mapper.readTree(resourceAsStream);
    StarTreeConfig config = StarTreeConfig.fromJson(jsonNode, new File(""));
    System.out.println(config);
  }
}
