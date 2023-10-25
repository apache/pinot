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
package org.apache.pinot.segment.local.segment.readers;

import java.io.IOException;
import java.util.HashMap;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A wrapper class to read column values of a row. The wrapper can either be over a {@link IndexSegment} and docId or
 * over a {@link GenericRow}<br>
 * The advantage of having wrapper over segment and docId is column values are read only when
 * {@link LazyRow#getValue(String)} is invoked.
 * This is useful to reduce the disk reads incurred due to loading the previous row during merge step.
 * There isn't any advantage to have a LazyRow wrap a GenericRow but has been kept for syntactic sugar.
 */
public class LazyRow {
    private IndexSegment _segment;
    private int _docId;
    private GenericRow _row;

    private HashMap<String, Object> _fieldToValueMap = new HashMap<>();

    public LazyRow() {
    }

    public LazyRow(GenericRow row) {
        _row = row;
    }

    public LazyRow(IndexSegment segment, int docId) {
        _segment = segment;
        _docId = docId;
    }

    public void setRow(GenericRow row) {
        _row = row;
    }

    public void init(IndexSegment segment, int docId) {
        this.clear();
        _segment = segment;
        _docId = docId;
    }

    public void init(GenericRow row) {
        this.clear();
        _row = row;
    }

    public Object getValue(String column) {

        if (_row != null) {
            if (_row.isNullValue(column)) {
                return null;
            }
            return _row.getValue(column);
        }
        return _fieldToValueMap.computeIfAbsent(column, col -> {
            Object value = null;
            try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_segment, col)) {
                if (!columnReader.isNull(_docId)) {
                    value = columnReader.getValue(_docId);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return value;
        });
    }

    public void clear() {
        _row = null;
        _fieldToValueMap.clear();
    }
}
