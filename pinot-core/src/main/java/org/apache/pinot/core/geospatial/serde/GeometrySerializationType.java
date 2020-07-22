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
package org.apache.pinot.core.geospatial.serde;

import org.apache.pinot.core.geospatial.GeometryType;


/**
 * The geometry type used in serialization
 */
public enum GeometrySerializationType {
  POINT(0, GeometryType.POINT),
  MULTI_POINT(1, GeometryType.MULTI_POINT),
  LINE_STRING(2, GeometryType.LINE_STRING),
  MULTI_LINE_STRING(3, GeometryType.MULTI_LINE_STRING),
  POLYGON(4, GeometryType.POLYGON),
  MULTI_POLYGON(5, GeometryType.MULTI_POLYGON),
  GEOMETRY_COLLECTION(6, GeometryType.GEOMETRY_COLLECTION);

  private final int _id;
  private final GeometryType _geometryType;

  GeometrySerializationType(int id, GeometryType geometryType) {
    _id = id;
    _geometryType = geometryType;
  }

  /**
   * @return the id of the serialization type
   */
  public int id() {
    return _id;
  }

  /**
   * @return the type in the geometry model
   */
  public GeometryType getGeometryType() {
    return _geometryType;
  }

  /**
   * Constructs the serialization type from the id
   * @param id id of the serialization type
   * @return the serialization type
   */
  public static GeometrySerializationType fromID(int id) {
    switch (id) {
      case 0:
        return POINT;
      case 1:
        return MULTI_POINT;
      case 2:
        return LINE_STRING;
      case 3:
        return MULTI_LINE_STRING;
      case 4:
        return POLYGON;
      case 5:
        return MULTI_POLYGON;
      case 6:
        return GEOMETRY_COLLECTION;
      default:
        throw new IllegalArgumentException("Invalid type id: " + id);
    }
  }}
