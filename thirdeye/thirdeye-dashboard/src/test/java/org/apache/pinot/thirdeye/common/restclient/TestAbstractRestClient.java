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

package org.apache.pinot.thirdeye.common.restclient;

import java.io.IOException;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAbstractRestClient extends AbstractRestClient {

  /**
   * Test compose url with null query parameters creates a valid url.
   */
  @Test
  public void testComposeUrlNullQueryParameters() throws IOException {
    String api = "/api/my/api";
    String host = "host";

    String actualUrl = composeUrl(host, api, null);
    String expectedUrl = String.format("http://%s%s", host, api);

    Assert.assertEquals(actualUrl,expectedUrl);
  }

  /**
   * Test compose url parameter with space create a valid url
   */
  @Test
  public void testComposeUrlGenericParameterWithSpaceAndSlash() throws IOException{
    String api = "/api/my/api";
    String host = "host";
    String parameterName = "parameter";
    String parameterValue = "param value";
    TreeMap<String, String> queryParameters = new TreeMap<String, String>();
    queryParameters.put(parameterName,parameterValue);

    String actualUrl = composeUrlGeneric(Protocol.HTTPS, host, api, queryParameters);
    String expectedUrl = "https://host/api/my/api?parameter=param%20value";

    Assert.assertEquals(actualUrl,expectedUrl);
  }
}
