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
package org.apache.pinot.spi.cursors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * ResponseSerde is used to serialize and deserialize responses for the Cursor Response Store.
 */
public interface ResponseSerde {
  /**
   * Get the type of response. The type is used to identify the serde implementation. Type has to be unique.
   * @return Type of the serde.
   */
  String getType();

  /**
   * Initialize the Serde from the configuration. The function is called with subset config of
   * pinot.broker.cursor.response.store.&lt;serde&gt;
   * @param pinotConfiguration Subset configuration of the Serde
   */
  void init(PinotConfiguration pinotConfiguration);

  /**
   * Serialize an object to the output stream
   * @param object Object to be serialized
   * @param stream OutputStream to write to.
   * @throws IOException Throws an exception when there is an issue writing to the output stream.
   */
  void serialize(Object object, OutputStream stream)
      throws IOException;

  /**
   * Deserialize an input stream to an object of a specific class.
   * @param stream Input stream to read from.
   * @param valueType Class of the object
   * @return An object of type T class
   * @param <T> Class Type
   * @throws IOException Thrown if there is an issue to read from the input stream.
   */
  <T> T deserialize(InputStream stream, Class<T> valueType)
      throws IOException;
}
