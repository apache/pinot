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
package org.apache.pinot.common.response;

import java.util.List;
import org.apache.pinot.spi.exception.QueryErrorCode;

/// The block used for broker response.
///
/// There are 3 types of blocks:
/// - [Success]: indicates the successful end of the response.
/// - [Error]: indicates an error in processing the request.
/// - [Data]: contains the actual data rows.
///
/// While [Success] and [Error] classes are included here, the [Data] class is an interface that can be implemented in
/// different ways depending on the data source.
///
/// [Data] implementations may use offheap memory, so it is important to call [#close] to release any resources when
/// blocks are no longer needed.
public interface BrokerBlock extends AutoCloseable {

  /// Indicates the successful end of the response.
  ///
  /// This block does not contain any data and is used to signal that all data blocks have been sent.
  /// Given they do not hold any resources, this class is implemented as a singleton, and the [#close] method is a no-op
  class Success implements BrokerBlock {
    public static final Success INSTANCE = new Success();

    private Success() {
    }

    @Override
    public void close() {
    }
  }

  /// Indicates an error in processing the request.
  ///
  /// This block contains an error code and an error message.
  class Error implements BrokerBlock {
    private final int _errorCode;
    private final String _errorMessage;

    public Error(QueryErrorCode errorCode, String errorMessage) {
      _errorCode = errorCode.getId();
      _errorMessage = errorMessage;
    }

    public Error(int errorCode, String errorMessage) {
      _errorCode = errorCode;
      _errorMessage = errorMessage;
    }

    public int getErrorCode() {
      return _errorCode;
    }

    public String getErrorMessage() {
      return _errorMessage;
    }

    @Override
    public void close()
        throws Exception {
    }
  }

  /// The data block containing actual data rows.
  ///
  /// Data can be accessed in a cursor-like fashion using [#next] and [#get] and can be read only once.
  interface Data extends BrokerBlock {
    int getNumRows();

    /// Returns the value at the given column index for the current row.
    ///
    /// The returned value must be compatible with the
    /// [external type of the column](org.apache.pinot.common.utils.DataSchema.ColumnDataType#toExternal(Object))
    Object get(int colIdx);

    boolean next();

    /// A [Data] block that is backed by a list of object arrays.
    class FromObjectArrList implements Data {
      private final List<Object[]> _rows;
      private int _currentId = 0;

      public FromObjectArrList(List<Object[]> rows) {
        _rows = rows;
      }

      @Override
      public int getNumRows() {
        return _rows.size();
      }

      @Override
      public Object get(int colIdx) {
        return _rows.get(_currentId - 1)[colIdx];
      }

      @Override
      public boolean next() {
        return _currentId++ < getNumRows();
      }

      @Override
      public void close() {
      }
    }
  }
}
