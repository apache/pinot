/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import javax.annotation.Nonnull;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Java serialization/de-serialization based DataTableSerDe implementation.
 */
public class DataTableJavaSerDe implements DataTableSerDe {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableJavaSerDe.class);

  /**
   * @param object Object to serialize
   * @return Serialized byte-array of the object
   */
  @Override
  public byte[] serialize(Object object) {
    return serializeJavaObject(object);
  }

  /**
   * @param bytes Serialized byte-array
   * @param dataType Enum type of the object
   * @param <T> Type of the object
   * @return De-serialized object
   */
  @Override
  public <T extends Serializable> T deserialize(byte[] bytes, DataType dataType) {
    return deserializeJavaObject(bytes);
  }

  /**
   * Given an object, returns its enum {@link com.linkedin.pinot.common.utils.DataTableSerDe.DataType}
   * @param object Object for which to get the data type.
   * @return DataType of the object.
   */
  @Override
  public DataType getObjectType(Object object) {
    return DataType.Object;
  }

  /**
   * Helper method to serialize an object using Java ser/de.
   *
   * @param object  to serialize
   * @return Serialized byte[] for the object.
   */
  public static byte[] serializeJavaObject(@Nonnull Object object) {
    byte[] bytes;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;

    try {
      try {
        out = new ObjectOutputStream(bos);
        out.writeObject(object);
      } catch (IOException e) {
        LOGGER.error("Caught exception while serializing object for data table.", e);
        Utils.rethrowException(e);
      }

      bytes = bos.toByteArray();
    } finally {
      IOUtils.closeQuietly((Closeable) out);
      IOUtils.closeQuietly(bos);
    }

    return bytes;
  }

  /**
   * Helper method to de-serialize an object using Java ser/de.
   * @param bytes Java serialized byte-array of the object.
   * @param <T> Type of the object
   * @return De-serialized object
   */
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T deserializeJavaObject(@Nonnull byte[] bytes) {

    final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    ObjectInputStream objectInputStream = null;
    Object readObject;

    try {
      try {
        objectInputStream = new ObjectInputStream(byteArrayInputStream);
        readObject = objectInputStream.readObject();
      } catch (final Exception e) {
        throw new RuntimeException("Caught exception while deserializing DataTable: " + e);
      }
    } finally {
      IOUtils.closeQuietly(objectInputStream);
      IOUtils.closeQuietly(byteArrayInputStream);
    }

    return (T) readObject;
  }
}
