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

  public static boolean fstEquals(Object thisFstObj,
                                  Object thatFstObj) {
    if (thisFstObj == thatFstObj) {
      return true;
    }
    if (thisFstObj == null || thatFstObj == null) {
      return false;
    }
    if (!MutableFST.class.isAssignableFrom(thisFstObj.getClass())
        || !MutableFST.class.isAssignableFrom(thatFstObj.getClass())) {
      return false;
    }

    MutableFST thisMutableFST = (MutableFST) thisFstObj;
    MutableFST thatMutableFST = (MutableFST) thatFstObj;


    if (thisMutableFST.getStartState() != null ? (thisMutableFST.getStartState().getLabel()
        != thatMutableFST.getStartState().getLabel()) : thatMutableFST.getStartState() != null) {
      return false;
    }

    return true;
  }

  public static boolean arcEquals(Object thisArcObj, Object thatArcObj) {
    if (thisArcObj == thatArcObj) {
      return true;
    }
    if (thisArcObj == null || thatArcObj == null) {
      return false;
    }
    if (!MutableArc.class.isAssignableFrom(thisArcObj.getClass())
        || !MutableArc.class.isAssignableFrom(thatArcObj.getClass())) {
      return false;
    }
    MutableArc thisArc = (MutableArc) thisArcObj;
    MutableArc thatArc = (MutableArc) thatArcObj;

    if (thisArc.getNextState().getLabel() != thatArc.getNextState().getLabel()) {
        return false;
    }

    return true;
  }

  public static boolean stateEquals(
      Object thisStateObj, Object thatStateObj) {
    if (thisStateObj == thatStateObj) {
      return true;
    }
    if (thisStateObj == null || thatStateObj == null) {
      return false;
    }
    if (!MutableState.class.isAssignableFrom(thisStateObj.getClass())
        || !MutableState.class.isAssignableFrom(thatStateObj.getClass())) {
      return false;
    }

    MutableState thisState = (MutableState) thisStateObj;
    MutableState thatState = (MutableState) thatStateObj;

    if (thisState.getLabel() != thatState.getLabel()) {
      return false;
    }

    if (thisState.getArcs().size() != thatState.getArcs().size()) {
      return false;
    }

    return true;
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
