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

import java.io.IOException;
import java.util.Objects;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.utils.idset.IdSet;


/**
 * Predicate for IN_ID_SET.
 */
public class InIdSetPredicate implements Predicate {
  public static final String ID_SET_INDICATOR = "__IDSET__";

  private final ExpressionContext _lhs;
  private final IdSet _idSet;

  public InIdSetPredicate(ExpressionContext lhs, IdSet idSet) {
    _lhs = lhs;
    _idSet = idSet;
  }

  @Override
  public Type getType() {
    return Type.IN_ID_SET;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  public IdSet getIdSet() {
    return _idSet;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InIdSetPredicate)) {
      return false;
    }
    InIdSetPredicate that = (InIdSetPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_idSet, that._idSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _idSet);
  }

  @Override
  public String toString() {
    try {
      return _lhs + " IN ('__IDSET__','" + _idSet.toBase64String() + "')";
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while serializing the IdSet", e);
    }
  }
}
