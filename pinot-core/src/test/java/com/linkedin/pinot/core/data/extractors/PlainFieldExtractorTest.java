/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.core.data.GenericRow;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.Schema.SchemaBuilder;


public class PlainFieldExtractorTest {
  @Test
  public void test1() throws Exception {
    System.out.println("Running PlainFieldExtractorTest");
    Schema schema =
        new SchemaBuilder().addSingleValueDimension("svDimensionInt", DataType.INT)
            .addSingleValueDimension("svDimensionDouble", DataType.DOUBLE)
            .addMultiValueDimension("mvDimension", DataType.STRING, ",")
            .addMetric("metric", DataType.INT)
            .addTime("incomingTime", TimeUnit.DAYS, DataType.LONG).build();
    PlainFieldExtractor extractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    Short shortObj = new Short((short) 5);
    fieldMap.put("svDimensionInt", shortObj);
    Float floatObj = new Float((float) 3.2);
    fieldMap.put("svDimensionDouble", floatObj);
    Double doubleObj = new Double((double) 34.5);
    fieldMap.put("metric", doubleObj);
    row.init(fieldMap);
    extractor.transform(row);
    Assert.assertTrue(row.getValue("svDimensionInt") instanceof Integer);
    Assert.assertTrue(row.getValue("svDimensionDouble") instanceof Double);
    Assert.assertTrue(row.getValue("mvDimension") != null);
    Assert.assertTrue(row.getValue("metric") instanceof Integer);
    Assert.assertTrue((Integer) row.getValue("metric") == 34);
    Assert.assertTrue(row.getValue("incomingTime") != null);
  }
}
