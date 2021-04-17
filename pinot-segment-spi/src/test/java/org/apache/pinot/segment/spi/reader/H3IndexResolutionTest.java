/**
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
package org.apache.pinot.segment.spi.reader;

import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


public class H3IndexResolutionTest {

  @Test
  public void testH3IndexResolution() {
    H3IndexResolution resolution = new H3IndexResolution(Lists.newArrayList(13, 5, 6));
    Assert.assertEquals(resolution.size(), 3);
    Assert.assertEquals(resolution.getLowestResolution(), 5);
    Assert.assertEquals(resolution.getResolutions(), Lists.newArrayList(5, 6, 13));
  }
}
