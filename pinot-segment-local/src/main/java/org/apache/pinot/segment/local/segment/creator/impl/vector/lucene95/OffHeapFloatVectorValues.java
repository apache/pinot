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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;


/**
 * Read the vector values from the index input. This supports both iterated and random access.
 */
abstract class OffHeapFloatVectorValues extends FloatVectorValues
    implements RandomAccessVectorValues<float[]> {

  protected final int _dimension;
  protected final int _size;
  protected final IndexInput _slice;
  protected final int _byteSize;
  protected int _lastOrd = -1;
  protected final float[] _value;

  OffHeapFloatVectorValues(int dimension, int size, IndexInput slice, int byteSize) {
    _dimension = dimension;
    _size = size;
    _slice = slice;
    _byteSize = byteSize;
    _value = new float[dimension];
  }

  @Override
  public int dimension() {
    return _dimension;
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public float[] vectorValue(int targetOrd)
      throws IOException {
    if (_lastOrd == targetOrd) {
      return _value;
    }
    _slice.seek((long) targetOrd * _byteSize);
    _slice.readFloats(_value, 0, _value.length);
    _lastOrd = targetOrd;
    return _value;
  }

  static OffHeapFloatVectorValues load(
      Lucene95HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData)
      throws IOException {
    if (fieldEntry._docsWithFieldOffset == -2
        || fieldEntry._vectorEncoding != VectorEncoding.FLOAT32) {
      return new EmptyOffHeapVectorValues(fieldEntry._dimension);
    }
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry._vectorDataOffset, fieldEntry._vectorDataLength);
    int byteSize = fieldEntry._dimension * Float.BYTES;
    if (fieldEntry._docsWithFieldOffset == -1) {
      return new DenseOffHeapVectorValues(
          fieldEntry._dimension, fieldEntry._size, bytesSlice, byteSize);
    } else {
      return new SparseOffHeapVectorValues(fieldEntry, vectorData, bytesSlice, byteSize);
    }
  }

  abstract Bits getAcceptOrds(Bits acceptDocs);

  static class DenseOffHeapVectorValues extends OffHeapFloatVectorValues {

    private int _doc = -1;

    public DenseOffHeapVectorValues(int dimension, int size, IndexInput slice, int byteSize) {
      super(dimension, size, slice, byteSize);
    }

    @Override
    public float[] vectorValue()
        throws IOException {
      return vectorValue(_doc);
    }

    @Override
    public int docID() {
      return _doc;
    }

    @Override
    public int nextDoc()
        throws IOException {
      return advance(_doc + 1);
    }

    @Override
    public int advance(int target)
        throws IOException {
      assert docID() < target;
      if (target >= _size) {
        _doc = NO_MORE_DOCS;
        return _doc;
      }
      _doc = target;
      return _doc;
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
        throws IOException {
      return new DenseOffHeapVectorValues(_dimension, _size, _slice.clone(), _byteSize);
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapFloatVectorValues {
    private final DirectMonotonicReader _ordToDoc;
    private final IndexedDISI _disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput _dataIn;
    private final Lucene95HnswVectorsReader.FieldEntry _fieldEntry;

    public SparseOffHeapVectorValues(
        Lucene95HnswVectorsReader.FieldEntry fieldEntry,
        IndexInput dataIn,
        IndexInput slice,
        int byteSize)
        throws IOException {

      super(fieldEntry._dimension, fieldEntry._size, slice, byteSize);
      _fieldEntry = fieldEntry;
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(fieldEntry._addressesOffset, fieldEntry._addressesLength);
      _dataIn = dataIn;
      _ordToDoc = DirectMonotonicReader.getInstance(fieldEntry._meta, addressesData);
      _disi =
          new IndexedDISI(
              dataIn,
              fieldEntry._docsWithFieldOffset,
              fieldEntry._docsWithFieldLength,
              fieldEntry._jumpTableEntryCount,
              fieldEntry._denseRankPower,
              fieldEntry._size);
    }

    @Override
    public float[] vectorValue()
        throws IOException {
      return vectorValue(_disi.index());
    }

    @Override
    public int docID() {
      return _disi.docID();
    }

    @Override
    public int nextDoc()
        throws IOException {
      return _disi.nextDoc();
    }

    @Override
    public int advance(int target)
        throws IOException {
      assert docID() < target;
      return _disi.advance(target);
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
        throws IOException {
      return new SparseOffHeapVectorValues(_fieldEntry, _dataIn, _slice.clone(), _byteSize);
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) _ordToDoc.get(ord);
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      if (acceptDocs == null) {
        return null;
      }
      return new Bits() {
        @Override
        public boolean get(int index) {
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return _size;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapFloatVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, null, 0);
    }

    private int _doc = -1;

    @Override
    public int dimension() {
      return super.dimension();
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public float[] vectorValue()
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return _doc;
    }

    @Override
    public int nextDoc()
        throws IOException {
      return advance(_doc + 1);
    }

    @Override
    public int advance(int target)
        throws IOException {
      _doc = NO_MORE_DOCS;
      return _doc;
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }
  }
}
