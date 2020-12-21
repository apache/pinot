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
package org.apache.pinot.core.util.fst;

import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;


/**
 *  Builds FST using lucene org.apache.lucene.util.fst.Builder library. FSTBuilder requires all the key/values
 *  be added in sorted order.
 */
public class FSTBuilder {
  public static final Logger LOGGER = LoggerFactory.getLogger(FSTBuilder.class);
  private Builder<Long> _builder = new Builder<>(FST.INPUT_TYPE.BYTE4, PositiveIntOutputs.getSingleton());
  private IntsRefBuilder _scratch = new IntsRefBuilder();

  public static FST buildFST(SortedMap<String, Integer> input)
      throws IOException {
    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE4, fstOutput);

    IntsRefBuilder scratch = new IntsRefBuilder();
    for (Map.Entry<String, Integer> entry : input.entrySet()) {
      builder.add(Util.toUTF16(entry.getKey(), scratch), entry.getValue().longValue());
    }
    FST<Long> result = builder.finish();
    return result;
  }

  public void addEntry(String key, Integer value)
      throws IOException {
    _builder.add(Util.toUTF16(key, _scratch), value.longValue());
  }

  public FST done()
      throws IOException {
    return _builder.finish();
  }
}
