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
package org.apache.pinot.segment.local.segment.index.nullvalue;

import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.NullHandling;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class NullValueIndexTypeTest {

  @DataProvider(name = "provideCases")
  public Object[][] provideCases() {
    NullHandling.TableBased tableBased = NullHandling.TableBased.getInstance();
    NullHandling.ColumnBased defaultFalse = new NullHandling.ColumnBased(false);
    NullHandling.ColumnBased defaultTrue = new NullHandling.ColumnBased(true);
    return new Object[][] {
        // This is the semantic table, assuming a null bitmap buffer exists in the segment
        // NullHandling | setNullable | Index is enabled
        new Object[] {tableBased, null, IndexConfig.ENABLED},
        new Object[] {tableBased, false, IndexConfig.ENABLED},
        new Object[] {tableBased, true, IndexConfig.ENABLED},

        new Object[] {defaultFalse, null, IndexConfig.DISABLED},
        new Object[] {defaultFalse, false, IndexConfig.DISABLED},
        new Object[] {defaultFalse, true, IndexConfig.ENABLED},

        new Object[] {defaultTrue, null, IndexConfig.ENABLED},
        new Object[] {defaultTrue, false, IndexConfig.DISABLED},
        new Object[] {defaultTrue, true, IndexConfig.ENABLED}
    };
  }

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(IndexConfig expected) {
      Assert.assertEquals(getActualConfig("dimStr", StandardIndexes.nullValueVector()), expected);
    }

    @Test(dataProvider = "provideCases", dataProviderClass = NullValueIndexTypeTest.class)
    public void isEnabledWhenNullable(NullHandling nullHandling, Boolean fieldNullable, IndexConfig expected) {
      _schema.getOptions().setNullHandling(nullHandling);

      FieldSpec dimStr = _schema.getFieldSpecFor("dimStr");
      if (fieldNullable != null) {
        dimStr.setNullable(fieldNullable);
      }

      assertEquals(expected);
    }
  }
}
