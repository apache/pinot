package com.linkedin.thirdeye.rootcause.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

@Deprecated
public class DimensionEntityTest {
  @Test
  public void testFromDimension() {
    DimensionEntity e = DimensionEntity.fromDimension(1.0, "myname", "myvalue", "mytype");
    Assert.assertEquals(e.getUrn(), "thirdeye:dimension:myname:myvalue:mytype");
  }

  @Test
  public void testFromDimensionEncode() {
    DimensionEntity e = DimensionEntity.fromDimension(1.0, "my:name", "my=value", "mytype");
    Assert.assertEquals(e.getUrn(), "thirdeye:dimension:my%3Aname:my%3Dvalue:mytype");
  }

  @Test
  public void testFromURN() {
    DimensionEntity e = DimensionEntity.fromURN("thirdeye:dimension:myname:myvalue:mytype", 1.0);
    Assert.assertEquals(e.getName(), "myname");
    Assert.assertEquals(e.getValue(), "myvalue");
    Assert.assertEquals(e.getType(), "mytype");
  }

  @Test
  public void testFromURNDecode() {
    DimensionEntity e = DimensionEntity.fromURN("thirdeye:dimension:my%3Aname:my%3Dvalue:mytype", 1.0);
    Assert.assertEquals(e.getName(), "my:name");
    Assert.assertEquals(e.getValue(), "my=value");
    Assert.assertEquals(e.getType(), "mytype");
  }
}
