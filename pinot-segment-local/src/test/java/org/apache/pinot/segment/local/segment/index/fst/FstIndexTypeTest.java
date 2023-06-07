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
package org.apache.pinot.segment.local.segment.index.fst;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FstIndexTypeTest {

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(FstIndexConfig expected) {
      Assert.assertEquals(getActualConfig("dimStr", StandardIndexes.fst()), expected);
    }

    @Test
    public void oldFieldConfigNull()
        throws JsonProcessingException {
      _tableConfig.setFieldConfigList(null);

      assertEquals(FstIndexConfig.DISABLED);
    }

    @Test
    public void oldEmptyFieldConfig()
        throws JsonProcessingException {
      cleanFieldConfig();

      assertEquals(FstIndexConfig.DISABLED);
    }

    @Test
    public void oldFieldConfigNotFst()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : []\n"
          + " }");

      assertEquals(FstIndexConfig.DISABLED);
    }

    @Test
    public void oldFieldConfigFstNoType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");

      assertEquals(new FstIndexConfig(null));
    }

    @Test
    public void oldFieldConfigFstNullIndexingType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.setIndexingConfig(null);

      assertEquals(new FstIndexConfig(null));
    }

    @Test
    public void oldFieldConfigFstExplicitLuceneType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.getIndexingConfig().setFSTIndexType(FSTType.LUCENE);

      assertEquals(new FstIndexConfig(FSTType.LUCENE));
    }

    @Test
    public void oldFieldConfigFstExplicitNativeType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.getIndexingConfig().setFSTIndexType(FSTType.NATIVE);

      assertEquals(new FstIndexConfig(FSTType.NATIVE));
    }

    @Test
    public void newNative()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"fst\": {\n"
          + "          \"type\": \"NATIVE\""
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(new FstIndexConfig(FSTType.NATIVE));
    }

    @Test
    public void newLucene()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"fst\": {\n"
          + "          \"type\": \"LUCENE\""
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(new FstIndexConfig(FSTType.LUCENE));
    }

    @Test
    public void newEmpty()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"fst\": {\n"
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(new FstIndexConfig(null));
    }

    @Test
    public void oldToNewConfConversion()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.getIndexingConfig().setFSTIndexType(FSTType.NATIVE);
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimStr"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(FstIndexType.INDEX_DISPLAY_NAME));
      assertTrue(fieldConfig.getIndexTypes().isEmpty());
      assertNull(_tableConfig.getIndexingConfig().getFSTIndexType());
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.fst(), StandardIndexes.fst(), "Standard index should use the same as "
        + "the FstIndexType static instance");
  }
}
