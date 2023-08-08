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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.*;


public class TextIndexConfigTest {
  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"fst\": \"NATIVE\",\n"
        + "        \"rawValue\": \"fakeValue\",\n"
        + "        \"queryCache\": true,\n"
        + "        \"useANDForMultiTermQueries\": true,\n"
        + "        \"stopWordsInclude\": [\"a\"],\n"
        + "        \"stopWordsExclude\": [\"b\"]\n"
        + "}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getFstType(), FSTType.NATIVE, "Unexpected fst");
    assertEquals(config.getRawValueForTextIndex(), "fakeValue", "Unexpected rawValue");
    assertTrue(config.isEnableQueryCache(), "Unexpected queryCache");
    assertTrue(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertEquals(config.getStopWordsInclude(), Lists.newArrayList("a"), "Unexpected stopWordsInclude");
    assertEquals(config.getStopWordsExclude(), Lists.newArrayList("b"), "Unexpected stopWordsExclude");
  }
}
