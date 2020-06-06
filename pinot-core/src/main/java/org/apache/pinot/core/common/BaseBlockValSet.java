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
package org.apache.pinot.core.common;

/**
 * Abstract base class implementation for BlockValSet
 */
public abstract class BaseBlockValSet implements BlockValSet {

  @Override
  public int getIntValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytesValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValues(int docId, int[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLongValues(int docId, long[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFloatValues(int docId, float[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDoubleValues(int docId, double[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringValues(int docId, String[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getDictionaryIds(int[] docIds, int length, int[] dictIdBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getIntValues(int[] docIds, int length, int[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getLongValues(int[] docIds, int length, long[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getFloatValues(int[] docIds, int length, float[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getDoubleValues(int[] docIds, int length, double[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getStringValues(int[] docIds, int length, String[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getBytesValues(int[] docIds, int length, byte[][] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] getLongValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] getFloatValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] getDoubleValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getStringValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] getBytesValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException();
  }
}
