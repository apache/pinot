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
package org.apache.pinot.segment.spi.memory.unsafe;

import java.io.Closeable;
import java.io.IOException;


/**
 * A representation of some offheap memory previously allocated. This is mainly an address, a size and a way to
 * release it.
 */
public interface Memory extends Closeable {
   /**
    * The virtual address where the memory starts.
    */
   long getAddress();

   /**
    * The number of bytes that can be accessed starting from {@link #getAddress()}.
    */
   long getSize();

   /**
    * If the memory is backed by a file (like in a memory map file) it syncs the content between the memory and the
    * disk. Otherwise it does nothing.
    */
   void flush();

   /**
    * Close this object, releasing the reserved memory.
    */
   @Override
   void close()
       throws IOException;
}
