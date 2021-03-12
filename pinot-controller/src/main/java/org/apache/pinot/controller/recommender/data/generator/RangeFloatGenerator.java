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
package org.apache.pinot.controller.recommender.data.generator;

import java.util.Random;


public class RangeFloatGenerator implements Generator {
  private final float _start;
  private final float _end;
  private final float _delta;

  Random _randGen = new Random(System.currentTimeMillis());

  public RangeFloatGenerator(float r1, float r2) {
    _start = (r1 < r2) ? r1 : r2;
    _end = (r1 > r2) ? r1 : r2;

    _delta = _end - _start;
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    return (_start + (_delta * _randGen.nextFloat()));
  }
}
