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
package org.apache.pinot.common.restlet.resources;

public enum ValidDocIdsType {
  // Default validDocIds type. This indicates that the validDocIds bitmap is loaded from the snapshot from the
  // Pinot segment. UpsertConfig's 'enableSnapshot' must be enabled for this type.
  SNAPSHOT("snapshot"),
  // This indicates that the validDocIds bitmap is loaded from the real-time server.
  ON_HEAP("onHeap"),
  // This indicates that the validDocIds bitmap is read from the real-time server. The valid document ids here does
  // take account into the deleted records. UpsertConfig's 'deleteRecordColumn' must be provided for this type.
  ON_HEAP_WITH_DELETE("onHeapWithDelete");

  private final String _type;

  ValidDocIdsType(String type) {
    _type = type;
  }

  @Override
  public String toString() {
    return _type;
  }

  public static ValidDocIdsType fromString(String type) {
    for (ValidDocIdsType validDocIdsType : ValidDocIdsType.values()) {
      if (validDocIdsType.toString().equalsIgnoreCase(type)) {
        return validDocIdsType;
      }
    }
    throw new IllegalArgumentException("Invalid validDocIds type: " + type);
  }
}
