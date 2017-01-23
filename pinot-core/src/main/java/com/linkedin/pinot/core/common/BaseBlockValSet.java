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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec;


/**
 * Abstract base class implementation for BlockValSet
 */
public abstract class BaseBlockValSet implements BlockValSet {
  @Override
  public BlockValIterator iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public FieldSpec.DataType getValueType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumDocs() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outValues, int outStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getLongValues(int[] inDocIds, int inStartPos, int inDocIdsSize, long[] outValues, int outStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getFloatValues(int[] inDocIds, int inStartPos, int inDocIdsSize, float[] outValues, int outStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getDoubleValues(int[] inDocIds, int inStartPos, int inDocIdsSize, double[] outValues, int outStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getStringValues(int[] inDocIds, int inStartPos, int inDocIdsSize, String[] outValues, int outStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] getLongValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] getFloatValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] getDoubleValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getStringValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getDictionaryIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds,
      int outStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDictionaryIdsForDocId(int docId, int[] outputDictIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumberOfMVEntriesArray() {
    throw new UnsupportedOperationException();
  }
}
