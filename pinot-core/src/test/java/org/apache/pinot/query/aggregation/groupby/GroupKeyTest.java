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
package org.apache.pinot.query.aggregation.groupby;

import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GroupKeyTest {

  @Test
  public void testGetKeys() {
    GroupKeyGenerator.GroupKey groupKey = new GroupKeyGenerator.GroupKey();
    groupKey._stringKey = "foo\tbar\t";
    String[] keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 3);
    Assert.assertEquals(keys, new String[]{"foo", "bar", ""});

    groupKey._stringKey = "foo\t\tbar";
    keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 3);
    Assert.assertEquals(keys, new String[]{"foo", "", "bar"});

    groupKey._stringKey = "\tfoo\tbar";
    keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 3);
    Assert.assertEquals(keys, new String[]{"", "foo", "bar"});

    groupKey._stringKey = "foo\t\t";
    keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 3);
    Assert.assertEquals(keys, new String[]{"foo", "", ""});

    groupKey._stringKey = "\t\t";
    keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 3);
    Assert.assertEquals(keys, new String[]{"", "", ""});

    groupKey._stringKey = "";
    keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 1);
    Assert.assertEquals(keys, new String[]{""});

    groupKey._stringKey = "\t";
    keys = groupKey.getKeys();
    Assert.assertEquals(keys.length, 2);
    Assert.assertEquals(keys, new String[]{"", ""});
  }
}
