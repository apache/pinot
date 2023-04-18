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
package org.apache.pinot.segment.spi.index;

import java.io.IOException;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FieldIndexConfigsTest {

  @Test
  public void testToString()
      throws IOException {
    IndexType index1 = Mockito.mock(IndexType.class);
    Mockito.when(index1.getId()).thenReturn("index1");
    IndexConfig indexConf = new IndexConfig(false);
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder()
        .add(index1, indexConf)
        .build();

    Assert.assertEquals(fieldIndexConfigs.toString(), "{\"index1\":"
        + JsonUtils.objectToString(indexConf) + "}");
  }
}
