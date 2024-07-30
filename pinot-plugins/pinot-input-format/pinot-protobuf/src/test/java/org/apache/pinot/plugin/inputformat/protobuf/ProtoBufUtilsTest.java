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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.protobuf.Descriptors;
import java.net.URL;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.*;


public class ProtoBufUtilsTest {

  @DataProvider(name = "typeCases")
  public Object[][] typeCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, "String"},
        new Object[] {INT_FIELD, "Integer"},
        new Object[] {NULLABLE_LONG_FIELD, "Long"},
        new Object[] {DOUBLE_FIELD, "Double"},
        new Object[] {NULLABLE_FLOAT_FIELD, "Float"},
        new Object[] {NULLABLE_BYTES_FIELD, "ByteString"},
        new Object[] {BOOL_FIELD, "Boolean"},
        new Object[] {ENUM_FIELD, "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage.TestEnum"},
        new Object[] {NESTED_MESSAGE,
            "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage.NestedMessage"}
    };
  }

  @Test(dataProvider = "typeCases")
  public void testGetTypeStrFromProto(String fieldName, String javaType) {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    Assert.assertEquals(ProtoBufUtils.getTypeStrFromProto(fd), javaType);
  }

  @Test
  public void testGetTypeStrFromProto() throws Exception {
    URL jarFile = getClass().getClassLoader().getResource("complex_types.jar");
    ClassLoader clsLoader = ProtoBufCodeGenMessageDecoder.loadClass(jarFile.getPath());
    Descriptors.Descriptor desc = ProtoBufCodeGenMessageDecoder.getDescriptorForProtoClass(clsLoader,
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage");
    Assert.assertEquals(ProtoBufUtils.getFullJavaName(desc),
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage");
  }
}
