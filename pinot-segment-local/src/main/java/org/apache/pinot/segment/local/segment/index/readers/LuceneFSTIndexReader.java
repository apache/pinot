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
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
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
public class LuceneFSTIndexReader implements TextIndexReader {
  public static final Logger LOGGER = LoggerFactory.getLogger(LuceneFSTIndexReader.class);

  private final FST<Long> _fst;

  public LuceneFSTIndexReader(PinotDataBuffer pinotDataBuffer)
      throws IOException {
    PinotBufferIndexInput indexInput = new PinotBufferIndexInput("fst-index", pinotDataBuffer, 0L,
        pinotDataBuffer.size());
    FST.FSTMetadata<Long> metadata = FST.readMetadata(indexInput, PositiveIntOutputs.getSingleton());
    OffHeapFSTStore fstStore = new OffHeapFSTStore(indexInput, indexInput.getFilePointer(), metadata);
    _fst = FST.fromFSTReader(metadata, fstStore);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new RuntimeException("LuceneFSTIndexReader only supports getDictIds currently.");
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    try {
      MutableRoaringBitmap dictIds = new MutableRoaringBitmap();
      List<Long> matchingIds = regexMatch(searchQuery, _fst);
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

  private static List<Long> regexMatch(String regexQuery, FST<Long> fst)
      throws IOException {
    Automaton automaton = new RegExp(regexQuery).toAutomaton();
    if (automaton.getNumStates() == 0) {
      return Collections.emptyList();
    }

    List<Path<Long>> queue = new ArrayList<>();
    List<Long> matchingDictIds = new ArrayList<>();
    queue.add(new Path<>(0, fst.getFirstArc(new FST.Arc<>()), fst.outputs.getNoOutput(), new IntsRefBuilder()));

    FST.Arc<Long> scratchArc = new FST.Arc<>();
    FST.BytesReader fstReader = fst.getBytesReader();
    Transition transition = new Transition();
    while (!queue.isEmpty()) {
      Path<Long> path = queue.remove(queue.size() - 1);
      if (automaton.isAccept(path._state) && path._fstNode.isFinal()) {
        matchingDictIds.add(path._output);
      }

      int transitionCount = automaton.initTransition(path._state, transition);
      for (int i = 0; i < transitionCount; i++) {
        automaton.getNextTransition(transition);
        int min = transition.min;
        int max = transition.max;
        if (min == max) {
          FST.Arc<Long> nextArc = fst.findTargetArc(min, path._fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            queue.add(copyPath(transition.dest, nextArc, path, fst));
          }
        } else {
          FST.Arc<Long> nextArc = Util.readCeilArc(min, fst, path._fstNode, scratchArc, fstReader);
          while (nextArc != null && nextArc.label() <= max) {
            queue.add(copyPath(transition.dest, nextArc, path, fst));
            nextArc = nextArc.isLast() ? null : fst.readNextRealArc(nextArc, fstReader);
          }
        }
      }
    }
    return matchingDictIds;
  }

  private static Path<Long> copyPath(int automatonState, FST.Arc<Long> nextArc, Path<Long> currentPath, FST<Long> fst) {
    IntsRefBuilder input = new IntsRefBuilder();
    input.copyInts(currentPath._input.get());
    input.append(nextArc.label());
    return new Path<>(automatonState, new FST.Arc<Long>().copyFrom(nextArc),
        fst.outputs.add(currentPath._output, nextArc.output()), input);
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
