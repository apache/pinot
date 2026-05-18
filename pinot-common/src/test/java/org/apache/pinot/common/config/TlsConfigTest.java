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
package org.apache.pinot.common.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TlsConfigTest {
  @Test
  public void copyConstructorShouldCopyInsecureAndAllowedProtocols() {
    TlsConfig original = new TlsConfig();
    original.setInsecure(true);
    original.setAllowedProtocols(new String[]{"TLSv1.2", "TLSv1.3"});

    TlsConfig copy = new TlsConfig(original);
    Assert.assertTrue(copy.isInsecure());
    Assert.assertEquals(copy.getAllowedProtocols(), new String[]{"TLSv1.2", "TLSv1.3"});

    // Ensure returned array is a defensive copy
    String[] protocols = copy.getAllowedProtocols();
    protocols[0] = "MUTATED";
    Assert.assertEquals(copy.getAllowedProtocols(), new String[]{"TLSv1.2", "TLSv1.3"});
  }

  @Test
  public void equalsAndHashCodeShouldIncludeAllowedProtocols() {
    TlsConfig a = new TlsConfig();
    a.setInsecure(true);
    a.setAllowedProtocols(new String[]{"TLSv1.2"});

    TlsConfig b = new TlsConfig();
    b.setInsecure(true);
    b.setAllowedProtocols(new String[]{"TLSv1.2"});

    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());

    b.setAllowedProtocols(new String[]{"TLSv1.3"});
    Assert.assertNotEquals(a, b);
  }
}
