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
package org.apache.pinot.query.planner.serde;

import java.util.Map;
import org.apache.pinot.common.proto.Plan;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ProtoSerializationUtilsTest {

  @Test
  public void testMoveClass() {
    TestClass testClass = new TestClass();
    testClass._enum = TestEnum.VALUE1;
    Plan.ObjectField objectField = ProtoSerializationUtils.convertObjectToObjectField(testClass);

    // Regular case
    TestClass deserialized = new TestClass();
    ProtoSerializationUtils.setObjectFieldToObject(deserialized, objectField);
    assertEquals(deserialized._enum, TestEnum.VALUE1);

    // Set wrong class name for the enum field
    Plan.MemberVariableField enumField = objectField.getMemberVariablesMap().get("_enum");
    Plan.MemberVariableField enumFieldWithWrongClass = Plan.MemberVariableField.newBuilder().setObjectField(
        Plan.ObjectField.newBuilder().setObjectClassName("wrongClass")
            .putAllMemberVariables(enumField.getObjectField().getMemberVariablesMap())).build();
    Plan.ObjectField objectFieldWithWrongClass =
        Plan.ObjectField.newBuilder().setObjectClassName(objectField.getObjectClassName())
            .putAllMemberVariables(Map.of("_enum", enumFieldWithWrongClass)).build();
    TestClass deserializedWithWrongClass = new TestClass();
    ProtoSerializationUtils.setObjectFieldToObject(deserializedWithWrongClass, objectFieldWithWrongClass);
    assertEquals(deserializedWithWrongClass._enum, TestEnum.VALUE1);
  }

  private static class TestClass {
    @ProtoProperties
    private TestEnum _enum;
  }

  private enum TestEnum {
    VALUE1, VALUE2
  }
}
