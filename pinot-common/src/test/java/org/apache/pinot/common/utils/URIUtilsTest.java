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
package org.apache.pinot.common.utils;

import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class URIUtilsTest {

  @Test
  public void testGetUri() {
    assertEquals(URIUtils.getUri("http://foo/bar").toString(), "http://foo/bar");
    assertEquals(URIUtils.getUri("http://foo/bar", "table").toString(), "http://foo/bar/table");
    assertEquals(URIUtils.getUri("http://foo/bar", "table", "segment+%25").toString(),
        "http://foo/bar/table/segment+%25");
    assertEquals(URIUtils.getUri("/foo/bar", "table", "segment+%25").toString(), "file:/foo/bar/table/segment+%25");
    assertEquals(URIUtils.getUri("file:/foo/bar", "table", "segment+%25").toString(),
        "file:/foo/bar/table/segment+%25");
  }

  @Test
  public void testGetPath() {
    assertEquals(URIUtils.getPath("http://foo/bar"), "http://foo/bar");
    assertEquals(URIUtils.getPath("http://foo/bar", "table"), "http://foo/bar/table");
    assertEquals(URIUtils.getPath("http://foo/bar", "table", "segment+%25"), "http://foo/bar/table/segment+%25");
    assertEquals(URIUtils.getPath("/foo/bar", "table", "segment+%25"), "/foo/bar/table/segment+%25");
    assertEquals(URIUtils.getPath("file:/foo/bar", "table", "segment+%25"), "file:/foo/bar/table/segment+%25");
  }

  @Test
  public void testConstructDownloadUrl() {
    assertEquals(URIUtils.constructDownloadUrl("http://foo/bar", "table", "segment"),
        "http://foo/bar/segments/table/segment");
    assertEquals(URIUtils.constructDownloadUrl("http://foo/bar", "table", "segment %"),
        "http://foo/bar/segments/table/segment+%25");
  }

  @Test
  public void testEncodeDecode() {
    int numRounds = 1000;
    int maxPartLength = 10;
    Random random = new Random();
    for (int i = 0; i < numRounds; i++) {
      String randomString = RandomStringUtils.random(random.nextInt(maxPartLength + 1));
      assertEquals(URIUtils.decode(URIUtils.encode(randomString)), randomString);
    }
  }
}
