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
package org.apache.pinot.common.utils.helix;

import java.util.List;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class InstanceCapabilityTest {

  @Test
  public void testUniqueNamesPass() {
    List<InstanceCapability> capabilities = List.of(new InstanceCapability("FIRST", InstanceType.SERVER, 1),
        new InstanceCapability("SECOND", InstanceType.BROKER, 1));
    assertEquals(InstanceCapability.checkUniqueNames(capabilities), capabilities);
  }

  @Test
  public void testDuplicateNameFails() {
    List<InstanceCapability> capabilities = List.of(new InstanceCapability("SAME", InstanceType.SERVER, 1),
        new InstanceCapability("SAME", InstanceType.BROKER, 2));
    assertThrows(IllegalStateException.class, () -> InstanceCapability.checkUniqueNames(capabilities));
  }
}
