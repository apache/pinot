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
package org.apache.pinot.spi.data;

import java.math.BigDecimal;


public interface MultiValueVisitor<R> {

  R visitInt(int[] value);

  R visitLong(long[] value);

  R visitFloat(float[] value);

  R visitDouble(double[] value);

  R visitBigDecimal(BigDecimal[] value);

  R visitBoolean(boolean[] value);

  R visitTimestamp(long[] value);

  R visitString(String[] value);

  R visitJson(String[] value);

  R visitBytes(byte[][] value);

  default SingleValueVisitor<R> asSingleValueVisitor() {
    return new SingleValueVisitor<R>() {
      @Override
      public R visitInt(int value) {
        return MultiValueVisitor.this.visitInt(new int[] {value});
      }

      @Override
      public R visitLong(long value) {
        return MultiValueVisitor.this.visitLong(new long[] {value});
      }

      @Override
      public R visitFloat(float value) {
        return MultiValueVisitor.this.visitFloat(new float[] {value});
      }

      @Override
      public R visitDouble(double value) {
        return MultiValueVisitor.this.visitDouble(new double[] {value});
      }

      @Override
      public R visitBigDecimal(BigDecimal value) {
        return MultiValueVisitor.this.visitBigDecimal(new BigDecimal[] {value});
      }

      @Override
      public R visitBoolean(boolean value) {
        return MultiValueVisitor.this.visitBoolean(new boolean[] {value});
      }

      @Override
      public R visitTimestamp(long value) {
        return MultiValueVisitor.this.visitLong(new long[] {value});
      }

      @Override
      public R visitString(String value) {
        return MultiValueVisitor.this.visitString(new String[] {value});
      }

      @Override
      public R visitJson(String value) {
        return MultiValueVisitor.this.visitString(new String[] {value});
      }

      @Override
      public R visitBytes(byte[] value) {
        return MultiValueVisitor.this.visitBytes(new byte[][] {value});
      }
    };
  }
}
