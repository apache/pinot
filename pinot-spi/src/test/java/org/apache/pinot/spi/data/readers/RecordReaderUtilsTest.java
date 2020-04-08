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
package org.apache.pinot.spi.data.readers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class RecordReaderUtilsTest {

  @Test
  public void testConvertMultiValue() {
    FieldSpec fieldSpec = new DimensionFieldSpec("intMV", DataType.INT, false);

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, (Collection) null));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, (String[]) null));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Collections.emptyList()));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[0]));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Collections.singletonList(null)));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{null}));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Collections.singletonList("")));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{""}));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Arrays.asList(null, "")));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{null, ""}));

    assertEquals(RecordReaderUtils.convertMultiValue(fieldSpec, Arrays.asList(null, "", 123)), new Object[]{123});
    assertEquals(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{null, "", "123"}), new Object[]{123});
  }
}
