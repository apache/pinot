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
package org.apache.pinot.segment.local.utils;

import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashSink;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import java.math.BigDecimal;
import java.util.Optional;


public class UltraLogLogUtils {
  private UltraLogLogUtils() {
  }

  public static Hasher64 defaultHasher() {
    return Hashing.wyhashFinal4();
  }

  public static final HashFunnel<Object> OBJECT_FUNNEL = new HashFunnel<Object>() {
    public void put(Object o, HashSink hashSink) {
      if (o instanceof Integer) {
        hashSink.putInt((Integer) o);
      } else if (o instanceof Long) {
        hashSink.putLong((Long) o);
      } else if (o instanceof Float) {
        hashSink.putFloat((Float) o);
      } else if (o instanceof Double) {
        hashSink.putDouble((Double) o);
      } else if (o instanceof BigDecimal) {
        hashSink.putString(((BigDecimal) o).toString());
      } else if (o instanceof String) {
        hashSink.putString((String) o);
      } else if (o instanceof byte[]) {
        hashSink.putBytes((byte[]) o);
      } else {
        throw new IllegalArgumentException(
            "Unrecognised input type for UltraLogLog: " + o.getClass().getSimpleName());
      }
    }
  };

  public static Optional<Long> hashObject(Object obj) {
    return Optional.ofNullable(obj)
        .map(o -> defaultHasher().hashToLong(o, OBJECT_FUNNEL));
  }
}
