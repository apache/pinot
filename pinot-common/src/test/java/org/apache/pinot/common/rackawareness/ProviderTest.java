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

import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link Provider}
 */
public class ProviderTest {
  @Test
  public void testToString() {
    Provider provider = Provider.AZURE;
    Assert.assertEquals(provider.toString(), "azure");
  }

  @Test
  public void testFromValidString() {
    Optional<Provider> provider = Provider.fromString("azure");
    Assert.assertSame(provider.get(), Provider.AZURE);

    provider = Provider.fromString("AZURE");
    Assert.assertSame(provider.get(), Provider.AZURE);

    provider = Provider.fromString("aZure");
    Assert.assertSame(provider.get(), Provider.AZURE);
  }

  @Test void testFromInvalidString() {
    Optional<Provider> unknownProvider = Provider.fromString("unknown");
    Assert.assertFalse(unknownProvider.isPresent());
  }
}
