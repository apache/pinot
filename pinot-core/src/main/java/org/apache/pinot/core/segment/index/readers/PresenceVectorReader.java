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
package org.apache.pinot.core.segment.index.readers;

/**
 * Reader interface to read from an underlying Presence vector. This is
 * primarily used to check if a particular column value corresponding to
 * a document ID is null or not.
 */
public interface PresenceVectorReader {

    /**
     * Check if the given docId is present in the corresponding column
     *
     * @param docId specifies ID to check for presence
     * @return true if docId is present (non null). False otherwise
     */
    boolean isPresent(int docId);
}
