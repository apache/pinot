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
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class IndexKeyTest {
  @Test
  public void testCompareTo() {
    List<IndexKey> iks = Arrays
        .asList(new IndexKey("foo", StandardIndexes.inverted()), new IndexKey("bar", StandardIndexes.bloomFilter()),
            new IndexKey("foo", StandardIndexes.forward()), new IndexKey("bar", StandardIndexes.dictionary()),
            new IndexKey("baz", StandardIndexes.json()), new IndexKey("baz", StandardIndexes.fst()));
    Collections.sort(iks);
    assertEquals(iks, Arrays
        .asList(new IndexKey("bar", StandardIndexes.bloomFilter()), new IndexKey("bar", StandardIndexes.dictionary()),
            new IndexKey("baz", StandardIndexes.fst()), new IndexKey("baz", StandardIndexes.json()),
            new IndexKey("foo", StandardIndexes.forward()), new IndexKey("foo", StandardIndexes.inverted())));
  }
}
