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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OperatorTypeRegistryTest {

  @Test
  public void testAllBuiltInTypesRegistered() {
    for (MultiStageOperator.Type builtIn : MultiStageOperator.Type.values()) {
      OperatorTypeDescriptor descriptor = OperatorTypeRegistry.fromId(builtIn.getId());
      Assert.assertNotNull(descriptor, "Built-in type " + builtIn + " (id=" + builtIn.getId() + ") not in registry");
      Assert.assertSame(descriptor, builtIn,
          "Registry entry for id=" + builtIn.getId() + " should be the enum constant");
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

  @Test
  public void testRegisterPluginRejectsReservedId() {
    Map<Integer, OperatorTypeDescriptor> map = new HashMap<>();
    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> OperatorTypeRegistry.registerPlugin(
            descriptor(OperatorTypeDescriptor.PLUGIN_ID_FLOOR - 1, "reservedId"), map));
    Assert.assertTrue(exception.getMessage().contains("reserved"), exception.getMessage());
    Assert.assertTrue(map.isEmpty(), "Rejected descriptor must not be registered");
  }

  @Test
  public void testRegisterPluginRejectsDuplicateId() {
    Map<Integer, OperatorTypeDescriptor> map = new HashMap<>();
    OperatorTypeRegistry.registerPlugin(descriptor(OperatorTypeDescriptor.PLUGIN_ID_FLOOR, "first"), map);
    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> OperatorTypeRegistry.registerPlugin(descriptor(OperatorTypeDescriptor.PLUGIN_ID_FLOOR, "second"), map));
    Assert.assertTrue(exception.getMessage().contains("Duplicate operator type id"), exception.getMessage());
  }

  @Test
  public void testRegisterPluginAcceptsPluginId() {
    Map<Integer, OperatorTypeDescriptor> map = new HashMap<>();
    OperatorTypeDescriptor descriptor = descriptor(OperatorTypeDescriptor.PLUGIN_ID_FLOOR, "validPluginType");
    OperatorTypeRegistry.registerPlugin(descriptor, map);
    Assert.assertSame(map.get(OperatorTypeDescriptor.PLUGIN_ID_FLOOR), descriptor);
  }

  private static OperatorTypeDescriptor descriptor(int id, String name) {
    return new OperatorTypeDescriptor() {
      @Override
      public int getId() {
        return id;
      }

      @Override
      public String name() {
        return name;
      }

      @SuppressWarnings("rawtypes")
      @Override
      public Class getStatKeyClass() {
        return MultiStageOperator.Type.AGGREGATE.getStatKeyClass();
      }

      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
      }
    };
  }
}
