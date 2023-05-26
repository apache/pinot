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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.spi.utils.JsonUtils;
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

  @Test
  public void serialization()
      throws JsonProcessingException {
    H3IndexResolution resolution = new H3IndexResolution(Lists.newArrayList(13, 5, 6));
    String string = JsonUtils.objectToString(resolution);
    Assert.assertEquals(string, "[5,6,13]");
  }

  @Test
  public void deserializationList()
      throws JsonProcessingException {
    H3IndexResolution resolution =
        JsonUtils.stringToObject("[5,6,13]", H3IndexResolution.class);
    Assert.assertEquals(resolution.size(), 3);
    Assert.assertEquals(resolution.getLowestResolution(), 5);
    Assert.assertEquals(resolution.getResolutions(), Lists.newArrayList(5, 6, 13));
  }

  @Test
  public void deserializationShort()
      throws JsonProcessingException {
    H3IndexResolution resolution =
        JsonUtils.stringToObject("8288", H3IndexResolution.class);
    Assert.assertEquals(resolution.size(), 3);
    Assert.assertEquals(resolution.getLowestResolution(), 5);
    Assert.assertEquals(resolution.getResolutions(), Lists.newArrayList(5, 6, 13));
  }

  @Test
  public void deserializationLargeInt() {
    Assert.assertThrows(JsonParseException.class,
        () -> JsonUtils.stringToObject(Integer.toString(Integer.MAX_VALUE), H3IndexResolution.class));
  }

  @Test
  public void deserializationSmallInt() {
    Assert.assertThrows(JsonParseException.class,
        () -> JsonUtils.stringToObject(Integer.toString(Integer.MIN_VALUE), H3IndexResolution.class));
  }

  @Test
  public void deserializationObject() {
    Assert.assertThrows(JsonParseException.class,
        () -> JsonUtils.stringToObject("{}", H3IndexResolution.class));
  }
}
