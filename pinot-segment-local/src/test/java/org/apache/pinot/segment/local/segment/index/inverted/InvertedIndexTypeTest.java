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
package org.apache.pinot.segment.local.segment.index.inverted;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.EmptyIndexConf;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class InvertedIndexTypeTest {


  public class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(IndexDeclaration<EmptyIndexConf> expected) {
      Assert.assertEquals(getActualConfig("dimInt", InvertedIndexType.INSTANCE), expected);
    }

    @Test
    public void oldConfNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(IndexDeclaration.notDeclared(InvertedIndexType.INSTANCE));
    }

    @Test
    public void oldConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[]")
      );

      assertEquals(IndexDeclaration.declaredDisabled());
    }

    @Test
    public void oldConfEnabled()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[\"dimInt\"]"));

      assertEquals(IndexDeclaration.declared(EmptyIndexConf.INSTANCE));
    }

    @Test
    public void newConfDisableByDefault()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "    }\n"
          + "}");

      assertEquals(IndexDeclaration.notDeclared(InvertedIndexType.INSTANCE));
    }

    @Test
    public void newConfDisabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "      \"inverted\": false"
          + "    }\n"
          + "}"
      );

      assertEquals(IndexDeclaration.declaredDisabled());
    }

    @Test
    public void newConfEnabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "      \"inverted\": true"
          + "    }\n"
          + "}"
      );
      assertEquals(IndexDeclaration.declared(EmptyIndexConf.INSTANCE));
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.inverted(), InvertedIndexType.INSTANCE, "Inverted index should use the same as "
        + "the InvertedIndexType static instance");
  }
}