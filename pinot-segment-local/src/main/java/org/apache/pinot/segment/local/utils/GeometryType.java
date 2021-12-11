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

/**
 * The geometry type.
 */
public enum GeometryType {
  POINT(false, 0, "ST_Point"),
  MULTI_POINT(true, 1, "ST_MultiPoint"),
  LINE_STRING(false, 2, "ST_LineString"),
  MULTI_LINE_STRING(true, 3, "ST_MultiLineString"),
  POLYGON(false, 4, "ST_Polygon"),
  MULTI_POLYGON(true, 5, "ST_MultiPolygon"),
  GEOMETRY_COLLECTION(true, 6, "ST_GeomCollection");

  private static final GeometryType[] ID_TO_TYPE_MAP = new GeometryType[]{
      POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON, GEOMETRY_COLLECTION
  };
  private final boolean _multiType;
  private final int _id;
  private final String _name;

  GeometryType(boolean multiType, int id, String name) {
    _multiType = multiType;
    _id = id;
    _name = name;
  }

  public boolean isMultiType() {
    return _multiType;
  }

  public String getName() {
    return _name;
  }

  /**
   * @return the id of the serialization type
   */
  public int id() {
    return _id;
  }

  /**
   * Constructs the serialization type from the id
   * @param id id of the serialization type
   * @return the serialization type
   */
  public static GeometryType fromID(int id) {
    if (id > ID_TO_TYPE_MAP.length) {
      throw new IllegalArgumentException("Invalid type id: " + id);
    }
    return ID_TO_TYPE_MAP[id];
  }
}
