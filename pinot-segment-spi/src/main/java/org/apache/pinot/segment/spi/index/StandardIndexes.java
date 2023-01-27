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

package org.apache.pinot.segment.spi.index;

public class StandardIndexes {
  private StandardIndexes() {
  }

  public static IndexType<?, ?, ?> forward() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("forward");
  }

  public static IndexType<?, ?, ?> dictionary() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("dictionary");
  }

  public static IndexType<?, ?, ?> nullValueVector() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("nullable");
  }

  public static IndexType<?, ?, ?> bloomFilter() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("bloom");
  }

  public static IndexType<?, ?, ?> fst() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("fst");
  }

  public static IndexType<?, ?, ?> inverted() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("inverted");
  }

  public static IndexType<?, ?, ?> json() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("json");
  }

  public static IndexType<?, ?, ?> range() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("range");
  }

  public static IndexType<?, ?, ?> text() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("text");
  }

  public static IndexType<?, ?, ?> h3() {
    return IndexService.getInstance().getIndexTypeByIdOrThrow("h3");
  }
}
