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
package org.apache.pinot.core.geospatial;

/**
 * The geometry type.
 */
public enum GeometryType {

  POINT(false, "ST_Point"),
  MULTI_POINT(true, "ST_MultiPoint"),
  LINE_STRING(false, "ST_LineString"),
  MULTI_LINE_STRING(true, "ST_MultiLineString"),
  POLYGON(false, "ST_Polygon"),
  MULTI_POLYGON(true, "ST_MultiPolygon"),
  GEOMETRY_COLLECTION(true, "ST_GeomCollection");

  private final boolean _multitype;
  private final String _name;

  GeometryType(boolean multitype, String name) {
    _multitype = multitype;
    _name = name;
  }

  public boolean isMultitype() {
    return _multitype;
  }

  public String getName() {
    return _name;
  }
}
