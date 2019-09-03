/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.rootcause.impl;

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
