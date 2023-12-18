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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;

public class ColumnJsonParserException extends JsonParseException {
    /**
     * Exception type for parsing problems when
     * processing JSON content in a column
     * Sub-class of {@link com.fasterxml.jackson.core.JsonParseException}.
     */

    final String _columnName;

    protected ColumnJsonParserException(String columnName, JsonParseException jpe) {
        super(jpe.getProcessor(), jpe.getOriginalMessage(), jpe.getCause());
        _columnName = columnName;
    }

    /**
     * Default method overridden so that we can add column and location information
     */
    @Override
    public String getMessage() {
        return "Column: " +
                _columnName +
                "\n" +
                super.getMessage();
    }

    @Override
    public String toString() {
        return getClass().getName() + ": " + getMessage();
    }
}
