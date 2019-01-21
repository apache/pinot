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
package org.apache.pinot.common.data.objects;

import java.util.List;

import org.apache.pinot.common.data.PinotObject;

import com.google.common.collect.Lists;

public class TextObject implements PinotObject {

  byte[] _bytes;
  private static List<String> _FIELDS = Lists.newArrayList("Content");

  @Override
  public void init(byte[] bytes) {
    _bytes = bytes;
  }

  @Override
  public byte[] toBytes() {
    return _bytes;
  }

  @Override
  public List<String> getPropertyNames() {
    return _FIELDS;
  }

  @Override
  public Object getProperty(String field) {
    // TODO Auto-generated method stub
    return null;
  }

}
