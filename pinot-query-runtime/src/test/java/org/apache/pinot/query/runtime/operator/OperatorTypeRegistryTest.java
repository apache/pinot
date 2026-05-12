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
package org.apache.pinot.query.runtime.operator;

import org.testng.Assert;
import org.testng.annotations.Test;


public class OperatorTypeRegistryTest {

  @Test
  public void testAllBuiltInTypesRegistered() {
    for (MultiStageOperator.Type builtIn : MultiStageOperator.Type.values()) {
      OperatorTypeDescriptor descriptor = OperatorTypeRegistry.fromId(builtIn.getId());
      Assert.assertNotNull(descriptor, "Built-in type " + builtIn + " (id=" + builtIn.getId() + ") not in registry");
      Assert.assertSame(descriptor, builtIn, "Registry entry for id=" + builtIn.getId() + " should be the enum constant");
    }
  }

  @Test
  public void testRegistryContainsExactlyBuiltIns() {
    // No extra plugins on the test classpath: registry size == Type.values().length
    Assert.assertEquals(OperatorTypeRegistry.size(), MultiStageOperator.Type.values().length,
        "Registry should contain exactly the built-in types when no plugins are present");
  }

  @Test
  public void testFromIdUnknownReturnsNull() {
    // Pick an id well outside the current built-in range
    Assert.assertNull(OperatorTypeRegistry.fromId(9999));
    Assert.assertNull(OperatorTypeRegistry.fromId(-1));
  }

  @Test
  public void testBuiltInDescriptorMethodsDelegateToEnum() {
    MultiStageOperator.Type aggregate = MultiStageOperator.Type.AGGREGATE;
    OperatorTypeDescriptor descriptor = OperatorTypeRegistry.fromId(aggregate.getId());
    Assert.assertNotNull(descriptor);
    Assert.assertEquals(descriptor.getId(), aggregate.getId());
    Assert.assertEquals(descriptor.name(), aggregate.name());
    Assert.assertEquals(descriptor.getStatKeyClass(), aggregate.getStatKeyClass());
  }
}
