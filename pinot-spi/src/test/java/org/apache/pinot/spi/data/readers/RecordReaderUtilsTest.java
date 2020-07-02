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
import java.util.Collections;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class RecordReaderUtilsTest {

  @Test
  public void testConvertMultiValue() {

    assertNull(RecordReaderUtils.convertMultiValue(null));

    assertNull(RecordReaderUtils.convertMultiValue(Collections.emptyList()));

    assertNull(RecordReaderUtils.convertMultiValue(Collections.singletonList(null)));

    assertNull(RecordReaderUtils.convertMultiValue(Collections.singletonList("")));

    assertNull(RecordReaderUtils.convertMultiValue(Arrays.asList(null, "")));

    assertEquals(RecordReaderUtils.convertMultiValue(Arrays.asList(null, "", 123)), new Object[]{123});
  }
}
