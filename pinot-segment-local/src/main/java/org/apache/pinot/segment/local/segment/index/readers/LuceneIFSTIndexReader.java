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
package org.apache.pinot.segment.local.segment.index.readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.Util;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneIFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.PinotBufferIndexInput;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class loads FST index from PinotDataBuffer and creates a FST reader which
 * is used in finding matching results for regexp queries. Since FST index currently
 * stores dict ids as values this class only implements getDictIds method.
 *
 */
public class LuceneIFSTIndexReader implements TextIndexReader {
  public static final Logger LOGGER = LoggerFactory.getLogger(LuceneFSTIndexReader.class);

  private final FST<BytesRef> _ifst;

  public LuceneIFSTIndexReader(PinotDataBuffer pinotDataBuffer)
      throws IOException {
    PinotBufferIndexInput indexInput = new PinotBufferIndexInput("ifst-index", pinotDataBuffer, 0L,
        pinotDataBuffer.size());
    FST.FSTMetadata<BytesRef> metadata = FST.readMetadata(indexInput, ByteSequenceOutputs.getSingleton());
    OffHeapFSTStore fstStore = new OffHeapFSTStore(indexInput, indexInput.getFilePointer(), metadata);
    _ifst = FST.fromFSTReader(metadata, fstStore);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new RuntimeException("LuceneFSTIndexReader only supports getDictIds currently.");
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    try {
      MutableRoaringBitmap dictIds = new MutableRoaringBitmap();
      List<Long> matchingIds = regexMatch(searchQuery, _ifst);
      for (Long matchingId : matchingIds) {
        dictIds.add(matchingId.intValue());
      }
      return dictIds.toImmutableRoaringBitmap();
    } catch (Exception ex) {
      LOGGER.error("Error getting matching Ids from FST", ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void close()
      throws IOException {
    // Do Nothing
  }

  private static List<Long> regexMatch(String regexQuery, FST<BytesRef> ifst)
      throws IOException {
    Automaton automaton = new RegExp(regexQuery.toLowerCase(Locale.ROOT)).toAutomaton();
    if (automaton.getNumStates() == 0) {
      return Collections.emptyList();
    }

    List<Path<BytesRef>> queue = new ArrayList<>();
    List<Long> matchingDictIds = new ArrayList<>();
    queue.add(new Path<>(0, ifst.getFirstArc(new FST.Arc<>()), ifst.outputs.getNoOutput(), new IntsRefBuilder()));

    FST.Arc<BytesRef> scratchArc = new FST.Arc<>();
    FST.BytesReader fstReader = ifst.getBytesReader();
    Transition transition = new Transition();
    while (!queue.isEmpty()) {
      Path<BytesRef> path = queue.remove(queue.size() - 1);
      if (automaton.isAccept(path._state) && path._fstNode.isFinal()) {
        BytesRef output = path._fstNode.nextFinalOutput();
        BytesRef completeOutput = output != null && output.length > 0 ? ifst.outputs.add(path._output, output)
            : path._output;
        for (Integer dictionaryId : LuceneIFSTIndexCreator.deserializeDictionaryIds(completeOutput)) {
          matchingDictIds.add(dictionaryId.longValue());
        }
      }

      int transitionCount = automaton.initTransition(path._state, transition);
      for (int i = 0; i < transitionCount; i++) {
        automaton.getNextTransition(transition);
        int min = transition.min;
        int max = transition.max;
        if (min == max) {
          FST.Arc<BytesRef> nextArc = ifst.findTargetArc(min, path._fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            queue.add(copyPath(transition.dest, nextArc, path, ifst));
          }
        } else {
          FST.Arc<BytesRef> nextArc = Util.readCeilArc(min, ifst, path._fstNode, scratchArc, fstReader);
          while (nextArc != null && nextArc.label() <= max) {
            queue.add(copyPath(transition.dest, nextArc, path, ifst));
            nextArc = nextArc.isLast() ? null : ifst.readNextRealArc(nextArc, fstReader);
          }
        }
      }
    }
    return matchingDictIds;
  }

  private static Path<BytesRef> copyPath(int automatonState, FST.Arc<BytesRef> nextArc, Path<BytesRef> currentPath,
      FST<BytesRef> ifst) {
    IntsRefBuilder input = new IntsRefBuilder();
    input.copyInts(currentPath._input.get());
    input.append(nextArc.label());
    return new Path<>(automatonState, new FST.Arc<BytesRef>().copyFrom(nextArc),
        ifst.outputs.add(currentPath._output, nextArc.output()), input);
  }

  private static final class Path<T> {
    private final int _state;
    private final FST.Arc<T> _fstNode;
    private final T _output;
    private final IntsRefBuilder _input;

    private Path(int state, FST.Arc<T> fstNode, T output, IntsRefBuilder input) {
      _state = state;
      _fstNode = fstNode;
      _output = output;
      _input = input;
    }
  }
}
