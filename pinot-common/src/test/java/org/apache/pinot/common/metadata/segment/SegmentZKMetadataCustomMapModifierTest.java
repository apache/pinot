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
package org.apache.pinot.common.metadata.segment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SegmentZKMetadataCustomMapModifierTest {
  @Test
  public void testValidJsonModifier()
      throws IOException {
    SegmentZKMetadataCustomMapModifier modifier =
        new SegmentZKMetadataCustomMapModifier("{\"mapModifyMode\":\"UPDATE\",\"map\":{\"key\":\"value\"}}");

    Assert.assertEquals(modifier.modifyMap(new HashMap<>(Map.of("existing", "value"))),
        Map.of("existing", "value", "key", "value"));
  }

  @Test(dataProvider = "invalidJsonModifiers")
  public void testRejectsMalformedJsonModifier(String jsonModifier) {
    Assert.expectThrows(IOException.class, () -> new SegmentZKMetadataCustomMapModifier(jsonModifier));
  }

  @DataProvider(name = "invalidJsonModifiers")
  public Object[][] invalidJsonModifiers() {
    return new Object[][]{
        {"[]"},
        {"\"modifier\""},
        {"{}"},
        {"{\"mapModifyMode\":null,\"map\":{}}"},
        {"{\"mapModifyMode\":1,\"map\":{}}"},
        {"{\"mapModifyMode\":\"INVALID\",\"map\":{}}"},
        {"{\"mapModifyMode\":\"UPDATE\",\"map\":[]}"},
        {"{\"mapModifyMode\":\"UPDATE\",\"map\":\"value\"}"},
        {"{\"mapModifyMode\":\"UPDATE\",\"map\":{\"key\":1}}"},
        {"{\"mapModifyMode\":\"UPDATE\",\"map\":{\"key\":null}}"},
        {"{\"mapModifyMode\":\"UPDATE\",\"map\":{\"key\":{\"nested\":\"value\"}}}"}
    };
  }
}
