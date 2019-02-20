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
package org.apache.pinot.common.utils;

import org.apache.commons.lang3.ArrayUtils;


/**
 * Utility functions for primitive arrays.
 */
public class PrimitiveArrayUtils {
  /**
   * Turns the passed array into a primitive array, if necessary.
   *
   * @param array The array to convert
   * @return A primitive array
   */
  public static Object toPrimitive(Object array) {
    if (array instanceof int[] || array instanceof long[] || array instanceof short[] || array instanceof byte[]
        || array instanceof char[] || array instanceof float[] || array instanceof double[]
        || array instanceof boolean[]) {
      return array;
    } else if (array instanceof Integer[]) {
      return ArrayUtils.toPrimitive((Integer[]) array);
    } else if (array instanceof Long[]) {
      return ArrayUtils.toPrimitive((Long[]) array);
    } else if (array instanceof Short[]) {
      return ArrayUtils.toPrimitive((Short[]) array);
    } else if (array instanceof Byte[]) {
      return ArrayUtils.toPrimitive((Byte[]) array);
    } else if (array instanceof Character[]) {
      return ArrayUtils.toPrimitive((Character[]) array);
    } else if (array instanceof Float[]) {
      return ArrayUtils.toPrimitive((Float[]) array);
    } else if (array instanceof Double[]) {
      return ArrayUtils.toPrimitive((Double[]) array);
    } else if (array instanceof Boolean[]) {
      return ArrayUtils.toPrimitive((Boolean[]) array);
    } else if (array instanceof Object[]) {
      Object[] objectArray = (Object[]) array;

      if (objectArray.length == 0) {
        return array;
      }

      Object firstElement = objectArray[0];
      if (firstElement == null) {
        return array;
      } else if (firstElement instanceof Integer) {
        int[] newArray = new int[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Integer) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Long) {
        long[] newArray = new long[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Long) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Short) {
        short[] newArray = new short[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Short) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Byte) {
        byte[] newArray = new byte[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Byte) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Character) {
        char[] newArray = new char[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Character) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Float) {
        float[] newArray = new float[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Float) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Double) {
        double[] newArray = new double[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Double) objectArray[i];
        }

        return newArray;
      } else if (firstElement instanceof Boolean) {
        boolean[] newArray = new boolean[objectArray.length];

        for (int i = 0; i < newArray.length; i++) {
          newArray[i] = (Boolean) objectArray[i];
        }

        return newArray;
      } else {
        throw new IllegalArgumentException("First element of array is of unhandled type " + array.getClass());
      }
    } else {
      throw new IllegalArgumentException("Not an array, got object of type " + array.getClass());
    }
  }
}
