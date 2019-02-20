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
package org.apache.pinot.pql.parsers.utils;

import java.io.Serializable;


public class Pair<FIRST extends Serializable, SECOND extends Serializable> implements Serializable {
  private FIRST first;
  private SECOND second;

  public FIRST getFirst() {
    return first;
  }

  public void setFirst(FIRST first) {
    this.first = first;
  }

  public SECOND getSecond() {
    return second;
  }

  public void setSecond(SECOND second) {
    this.second = second;
  }

  public Pair(FIRST first, SECOND second) {
    super();
    this.first = first;
    this.second = second;
  }
}
