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
package org.apache.pinot.query.runtime.operator.join;

import java.util.AbstractList;
import java.util.List;
import javax.annotation.Nullable;


/**
 * This util class is a view over the left and right row joined together.
 * Currently, this is used for filtering and input of projection. So if the joined
 * tuple doesn't pass the predicate, the join result is not materialized into {@code Object[]}.
 */
public abstract class JoinedRowView extends AbstractList<Object> implements List<Object> {
  protected final int _leftSize;
  protected final int _size;

  protected JoinedRowView(int resultColumnSize, int leftSize) {
    _leftSize = leftSize;
    _size = resultColumnSize;
  }

  private static final class BothNotNullView extends JoinedRowView {
    private final Object[] _leftRow;
    private final Object[] _rightRow;

    private BothNotNullView(Object[] leftRow, Object[] rightRow, int resultColumnSize, int leftSize) {
      super(resultColumnSize, leftSize);
      _leftRow = leftRow;
      _rightRow = rightRow;
    }

    @Override
    public Object get(int i) {
      return i < _leftSize ? _leftRow[i] : _rightRow[i - _leftSize];
    }

    @Override
    public Object[] toArray() {
      Object[] resultRow = new Object[_size];
      System.arraycopy(_leftRow, 0, resultRow, 0, _leftSize);
      System.arraycopy(_rightRow, 0, resultRow, _leftSize, _rightRow.length);
      return resultRow;
    }
  }

  private static final class RightNotNullView extends JoinedRowView {
    private final Object[] _rightRow;

    public RightNotNullView(Object[] rightRow, int resultColumnSize, int leftSize) {
      super(resultColumnSize, leftSize);
      _rightRow = rightRow;
    }

    @Override
    public Object get(int i) {
      return i < _leftSize ? null : _rightRow[i - _leftSize];
    }

    @Override
    public Object[] toArray() {
      Object[] resultRow = new Object[_size];
      System.arraycopy(_rightRow, 0, resultRow, _leftSize, _rightRow.length);
      return resultRow;
    }
  }

  private static final class LeftNotNullView extends JoinedRowView {
    private final Object[] _leftRow;

    public LeftNotNullView(Object[] leftRow, int resultColumnSize, int leftSize) {
      super(resultColumnSize, leftSize);
      _leftRow = leftRow;
    }

    @Override
    public Object get(int i) {
      return i < _leftSize ? _leftRow[i] : null;
    }

    @Override
    public Object[] toArray() {
      Object[] resultRow = new Object[_size];
      System.arraycopy(_leftRow, 0, resultRow, 0, _leftSize);
      return resultRow;
    }
  }

  public static JoinedRowView of(@Nullable Object[] leftRow, @Nullable Object[] rightRow, int resultColumnSize,
      int leftSize) {
    if (leftRow == null && rightRow == null) {
      throw new IllegalStateException("both left and right side of join are null");
    }
    if (leftRow == null) {
      return new RightNotNullView(rightRow, resultColumnSize, leftSize);
    }
    if (rightRow == null) {
      return new LeftNotNullView(leftRow, resultColumnSize, leftSize);
    }
    return new BothNotNullView(leftRow, rightRow, resultColumnSize, leftSize);
  }

  @Override
  public int size() {
    return _size;
  }
}
