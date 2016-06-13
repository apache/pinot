/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.indexsegment;

/**
 * IndexType shows supported indexes from very top level. Now only supports
 * columnar index, which is GAZELLE_INDEX.
 * In the future can support more index types, like row based or tree based
 * structure.
 *
 *
 */
public enum IndexType {
  COLUMNAR,
  SIMPLE;

  public static IndexType valueOfStr(String value) {
    return IndexType.valueOf(value.toUpperCase());
  }
}
