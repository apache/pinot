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
package org.apache.pinot.core.geospatial.transform.function;

import org.apache.pinot.core.geospatial.GeometryUtils;
import org.locationtech.jts.geom.GeometryFactory;


/**
 * Constructor function for geography object from WKB.
 */
public class StGeogFromWKBFunction extends ConstructFromWKBFunction {
  public static final String FUNCTION_NAME = "ST_GeogFromWKB";

  @Override
  protected GeometryFactory getGeometryFactory() {
    return GeometryUtils.GEOGRAPHY_FACTORY;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }
}
