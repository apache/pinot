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
package org.apache.pinot.core.query.request.context.predicate;

import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.locationtech.jts.geom.Geometry;


//TODO: Make this flexible
public class GeoPredicate {

  //this is the column name
  ExpressionContext _lhs;

  Predicate _type;

  Geometry _geometry;

  double _distance;

  public enum Pre {
    WITHIN, OVERLAP;
  }

  public ExpressionContext getLhs() {
    return _lhs;
  }

  public void setLhs(ExpressionContext lhs) {
    _lhs = lhs;
  }

  public Predicate getType() {
    return _type;
  }

  public void setType(Predicate type) {
    _type = type;
  }

  public Geometry getGeometry() {
    return _geometry;
  }

  public void setGeometry(Geometry geometry) {
    _geometry = geometry;
  }

  public double getDistance() {
    return _distance;
  }

  public void setDistance(double distance) {
    _distance = distance;
  }
}
