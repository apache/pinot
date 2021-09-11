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
package org.apache.pinot.segment.local.segment.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class IndexKeyTest {
  @Test
  public void testCompareTo() {
    List<IndexKey> iks = Arrays
        .asList(new IndexKey("foo", ColumnIndexType.INVERTED_INDEX), new IndexKey("bar", ColumnIndexType.BLOOM_FILTER),
            new IndexKey("foo", ColumnIndexType.FORWARD_INDEX), new IndexKey("bar", ColumnIndexType.DICTIONARY),
            new IndexKey("baz", ColumnIndexType.JSON_INDEX), new IndexKey("baz", ColumnIndexType.FST_INDEX));
    Collections.sort(iks);
    assertEquals(iks, Arrays
        .asList(new IndexKey("bar", ColumnIndexType.DICTIONARY), new IndexKey("bar", ColumnIndexType.BLOOM_FILTER),
            new IndexKey("baz", ColumnIndexType.FST_INDEX), new IndexKey("baz", ColumnIndexType.JSON_INDEX),
            new IndexKey("foo", ColumnIndexType.FORWARD_INDEX), new IndexKey("foo", ColumnIndexType.INVERTED_INDEX)));
  }
}
