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

package org.apache.pinot.common.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaNullHandlingTest {

  @Test
  void tableMode()
      throws JsonProcessingException {
    String json = "{\"mode\":\"table\"}";
    Schema.NullHandling nullHandling = JsonUtils.stringToObject(json, Schema.NullHandling.class);

    Assert.assertSame(nullHandling, Schema.NullHandling.TableBased.getInstance());

    String serialized = JsonUtils.objectToString(nullHandling);
    Assert.assertEquals(serialized, json);
  }

  @Test
  void tableColumnDefaultFalse()
      throws JsonProcessingException {
    String json = "{\"mode\":\"column\",\"default\":false}";
    Schema.NullHandling nullHandling = JsonUtils.stringToObject(json, Schema.NullHandling.class);

    Assert.assertEquals(nullHandling, new Schema.NullHandling.ColumnBased(false));

    String serialized = JsonUtils.objectToString(nullHandling);
    Assert.assertEquals(serialized, json);
  }


  @Test
  void tableColumnDefaultTrue()
      throws JsonProcessingException {
    String json = "{\"mode\":\"column\",\"default\":true}";
    Schema.NullHandling nullHandling = JsonUtils.stringToObject(json, Schema.NullHandling.class);

    Assert.assertEquals(nullHandling, new Schema.NullHandling.ColumnBased(true));

    String serialized = JsonUtils.objectToString(nullHandling);
    Assert.assertEquals(serialized, json);
  }
}
