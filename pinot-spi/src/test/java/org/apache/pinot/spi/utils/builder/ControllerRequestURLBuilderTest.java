package org.apache.pinot.spi.utils.builder;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ControllerRequestURLBuilderTest {

  @Test
  public void testForConsumerWatermarksGet() {
    ControllerRequestURLBuilder builder = ControllerRequestURLBuilder.baseUrl("http://localhost:9000");
    assertEquals(builder.forConsumerWatermarksGet("myTable"),
        StringUtil.join("/", "http://localhost:9000", "tables", "myTable", "consumerWatermarks"));
  }
}
