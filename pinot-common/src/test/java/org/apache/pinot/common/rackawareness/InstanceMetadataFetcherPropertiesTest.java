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
package org.apache.pinot.common.rackawareness;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link InstanceMetadataFetcherProperties}
 */
public class InstanceMetadataFetcherPropertiesTest {
  @Test
  public void testConstructor() {
    InstanceMetadataFetcherProperties properties = new InstanceMetadataFetcherProperties(3, 600_000, 600_000);
    Assert.assertEquals(properties.getMaxRetry(), 3);
    Assert.assertEquals(properties.getConnectionTimeOut(), 600_000);
    Assert.assertEquals(properties.getRequestTimeOut(), 600_000);
  }

  @Test
  public void testConstructorCopy() {
    InstanceMetadataFetcherProperties properties = new InstanceMetadataFetcherProperties(
        new InstanceMetadataFetcherProperties(3, 600_000, 600_000)
    );
    Assert.assertEquals(properties.getMaxRetry(), 3);
    Assert.assertEquals(properties.getConnectionTimeOut(), 600_000);
    Assert.assertEquals(properties.getRequestTimeOut(), 600_000);
  }

}
