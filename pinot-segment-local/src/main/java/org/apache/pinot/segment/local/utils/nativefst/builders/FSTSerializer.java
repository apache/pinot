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
package org.apache.pinot.segment.local.utils.nativefst.builders;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.FSTFlags;


/**
 * All FST serializers (to binary formats) will implement this interface.
 */
public interface FSTSerializer {
  /**
   * Serialize a finite state automaton to an output stream.
   * 
   * @param FST The automaton to serialize.
   * @param os The output stream to serialize to.
   * @param <T> A subclass of {@link OutputStream}, returned for chaining. 
   * @return Returns <code>T</code> for chaining.
   * @throws IOException Rethrown if an I/O error occurs.
   */
  public <T extends OutputStream> T serialize(FST FST, T os) throws IOException;

  /**
   * @return Returns the set of flags supported by the serializer (and the output
   * automaton).
   */
  public Set<FSTFlags> getFlags();

  /**
   * Enables support for right language count on nodes, speeding up perfect hash
   * counts (only if {@link #getFlags()} returns {@link FSTFlags#NUMBERS}).
   * 
   * @return Returns <code>this</code> for call chaining.
   */
  public FSTSerializer withNumbers();
}
