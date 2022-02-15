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

package org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils;

import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableArc;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableState;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MutableFSTUtils {

  private MutableFSTUtils() {
  }

  public static boolean fstEquals(Object thisFstObj,
                                  Object thatFstObj) {
    if (thisFstObj == thatFstObj) {
      return true;
    }
    if (thisFstObj instanceof MutableFST && thatFstObj instanceof MutableFST) {
      MutableFST thisFST = (MutableFST) thisFstObj;
      MutableFST thatFST = (MutableFST) thatFstObj;
      return thisFST.getStartState().getLabel() == thatFST.getStartState().getLabel()
          && thisFST.getStartState().getArcs().size() == thatFST.getStartState().getArcs().size();
    }
    return false;
  }

  public static boolean arcEquals(Object thisArcObj, Object thatArcObj) {
    if (thisArcObj == thatArcObj) {
      return true;
    }
    if (thisArcObj instanceof MutableArc && thatArcObj instanceof MutableArc) {
      MutableArc thisArc = (MutableArc) thisArcObj;
      MutableArc thatArc = (MutableArc) thatArcObj;
      return thisArc.getNextState().getLabel() == thatArc.getNextState().getLabel()
          && thisArc.getNextState().getArcs().size() == thatArc.getNextState().getArcs().size();
    }
    return false;
  }

  public static boolean stateEquals(Object thisStateObj, Object thatStateObj) {
    if (thisStateObj == thatStateObj) {
      return true;
    }
    if (thisStateObj instanceof MutableState && thatStateObj instanceof MutableState) {
      MutableState thisState = (MutableState) thisStateObj;
      MutableState thatState = (MutableState) thatStateObj;
      return thisState.getLabel() == thatState.getLabel() && thisState.getArcs().size() == thatState.getArcs().size();
    }
    return false;
  }

  /**
   * Return number of matches for given regex for realtime FST
   */
  public static long regexQueryNrHitsForRealTimeFST(String regex, MutableFST fst) {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch(regex, fst, writer::add);
    return writer.get().getCardinality();
  }
}
