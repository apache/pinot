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
    final static long serialVersionUID = 123; // Stupid eclipse...

    final String _columnName;

    protected ColumnJsonParserException(String columnName, String msg, JsonLocation loc, Throwable rootCause) {
        /* Argh. IOException(Throwable,String) is only available starting
         * with JDK 1.6...
         */
        super(msg, loc, rootCause);
        _columnName = columnName;
    }

    /**
     * Default method overridden so that we can add location information
     */
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Column: ");
        sb.append(_columnName);
        sb.append("\n");
        sb.append(super.getMessage());
        return sb.toString();
    }

    @Override
    public String toString() {
        return getClass().getName() + ": " + getMessage();
    }
}
