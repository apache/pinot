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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonUtilsTestSamplePojo {
  int _primitiveIntegerField;
  String _stringField;
  Long _longField;
  Clazz _classField;

  public int getPrimitiveIntegerField() {
    return _primitiveIntegerField;
  }

  public void setPrimitiveIntegerField(int primitiveIntegerField) {
    _primitiveIntegerField = primitiveIntegerField;
  }

  public String getStringField() {
    return _stringField;
  }

  public void setStringField(String stringField) {
    _stringField = stringField;
  }

  public Long getLongField() {
    return _longField;
  }

  public void setLongField(Long longField) {
    _longField = longField;
  }

  public Clazz getClassField() {
    return _classField;
  }

  public void setClassField(Clazz classField) {
    _classField = classField;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public class Clazz {
    Integer _internalIntField;
    String _internalStringField;

    public int getInternalIntField() {
      return _internalIntField;
    }

    public void setInternalIntField(int internalIntField) {
      _internalIntField = internalIntField;
    }

    public String getInternalStringField() {
      return _internalStringField;
    }

    public void setInternalStringField(String internalStringField) {
      _internalStringField = internalStringField;
    }
  }
}
