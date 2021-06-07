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
package org.apache.pinot.spi.config.table;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class UpsertConfigTest {

  @Test
  public void testUpsertConfig() {
    UpsertConfig upsertConfig1 = new UpsertConfig(UpsertConfig.Mode.FULL);
    assertEquals(upsertConfig1.getMode(), UpsertConfig.Mode.FULL);

    UpsertConfig upsertConfig2 = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    assertEquals(upsertConfig2.getMode(), UpsertConfig.Mode.PARTIAL);
  }
}
