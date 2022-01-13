/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.Arc;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.State;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.SymbolTable;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Atri Sharma
 */
public class MutableFSTUtils {

  private static final Logger log = LoggerFactory.getLogger(MutableFSTUtils.class);

  public interface EqualsReporter {
    void report(String msg, Object a, Object b);
  }

  public static final EqualsReporter NULL_REPORTER = new EqualsReporter() {
    @Override
    public void report(String msg, Object a, Object b) {
      // nothing
    }
  };

  public static final EqualsReporter LOG_REPORTER = new EqualsReporter() {
    @Override
    public void report(String msg, Object a, Object b) {
      String aa = objToString(a);
      String bb = objToString(b);
      log.info("Equals difference: " + msg + " " + aa + " " + bb);
    }

    private String objToString(Object a) {
      String aa = "<null>";
      if (a != null) {
        aa = a.toString();
        if (aa.length() > 80) {
          aa = "\n" + aa;
        }
      }
      return aa;
    }
  };

  public static boolean fstEquals(Object thisFstObj, Object thatFstObj) {
    return fstEquals(thisFstObj, thatFstObj, Double.MIN_VALUE);
  }

  public static boolean fstEquals(Object thisFstObj, Object thatFstObj, EqualsReporter reporter) {
    return fstEquals(thisFstObj, thatFstObj, Double.MIN_VALUE, reporter);
  }

  public static boolean fstEquals(Object thisFstObj, Object thatFstObj,
                                  double epsilon) {
    return fstEquals(thisFstObj, thatFstObj, epsilon, NULL_REPORTER);
  }

  public static boolean fstEquals(Object thisFstObj,
                                  Object thatFstObj,
                                  double epsilon,
                                  EqualsReporter reporter) {
    if (thisFstObj == thatFstObj) {
      return true;
    }
    if (thisFstObj == null || thatFstObj == null) {
      return false;
    }
    if (!MutableFST.class.isAssignableFrom(thisFstObj.getClass()) || !MutableFST.class.isAssignableFrom(thatFstObj.getClass())) {
      return false;
    }

    MutableFST thisMutableFST = (MutableFST) thisFstObj;
    MutableFST thatMutableFST = (MutableFST) thatFstObj;


    if (thisMutableFST.getStartState() != null ? (thisMutableFST.getStartState().getLabel() != thatMutableFST.getStartState().getLabel()) : thatMutableFST.getStartState() != null) {
      reporter.report("fst.startstate", thisMutableFST.getStartState(), thatMutableFST.getStartState());
      return false;
    }

    return true;
  }

  public static boolean arcEquals(Object thisArcObj, Object thatArcObj) {
    return arcEquals(thisArcObj, thatArcObj, Double.MIN_VALUE);
  }

  public static boolean arcEquals(Object thisArcObj, Object thatArcObj,
                                  double epsilon) {
    if (thisArcObj == thatArcObj) {
      return true;
    }
    if (thisArcObj == null || thatArcObj == null) {
      return false;
    }
    if (!Arc.class.isAssignableFrom(thisArcObj.getClass()) || !Arc.class.isAssignableFrom(thatArcObj.getClass())) {
      return false;
    }
    Arc thisArc = (Arc) thisArcObj;
    Arc thatArc = (Arc) thatArcObj;

    if (thisArc.getNextState().getLabel() != thatArc.getNextState().getLabel()) {
        return false;
    }

    return true;
  }

  public static boolean stateEquals(Object thisStateObj, Object thatStateObj) {
    return stateEquals(thisStateObj, thatStateObj, Double.MIN_VALUE);
  }

  public static boolean stateEquals(Object thisStateObj, Object thatStateObj, double epsilon) {
    return stateEquals(thisStateObj, thatStateObj, epsilon, NULL_REPORTER);
  }

  public static boolean stateEquals(
      Object thisStateObj, Object thatStateObj, double epsilon, EqualsReporter reporter) {
    if (thisStateObj == thatStateObj) {
      return true;
    }
    if (thisStateObj == null || thatStateObj == null) {
      return false;
    }
    if (!State.class.isAssignableFrom(thisStateObj.getClass()) || !State.class.isAssignableFrom(thatStateObj.getClass())) {
      return false;
    }

    State thisState = (State) thisStateObj;
    State thatState = (State) thatStateObj;

    if (thisState.getLabel() != thatState.getLabel()) {
      reporter.report("state.id", thisState.getLabel(), thatState.getLabel());
      return false;
    }

    if (thisState.getArcs().size() != thatState.getArcs().size()) {
      reporter.report("state.arcCount", thisState.getArcCount(), thatState.getArcCount());
      return false;
    }
    for (int i = 0; i < thisState.getArcs().size(); i++) {
      Arc thisArc = thisState.getArc(i);
      Arc thatArc = thatState.getArc(i);
      if (!arcEquals(thisArc, thatArc, epsilon)) {
        reporter.report("state.arc", thisArc, thatArc);
        return false;
      }
    }
    return true;
  }

  public static boolean symbolTableEquals(Object thisSyms, Object thatSyms) {
    return symbolTableEquals(thisSyms, thatSyms, NULL_REPORTER);
  }

  public static boolean symbolTableEquals(Object thisSyms,
                                          Object thatSyms,
                                          EqualsReporter reporter) {
    if (thisSyms == thatSyms) {
      return true;
    }
    if (thisSyms == null || thatSyms == null) {
      return false;
    }
    if (!SymbolTable.class.isAssignableFrom(thisSyms.getClass()) || !SymbolTable.class.isAssignableFrom(thatSyms.getClass())) {
      reporter.report("symbolTable.isAssignable", thisSyms, thatSyms);
      return false;
    }

    SymbolTable thisS = (SymbolTable) thisSyms;
    SymbolTable thatS = (SymbolTable) thatSyms;

    if (thisS.size() != thatS.size()) {
      reporter.report("symbolTable.size", thisS.size(), thatS.size());
      return false;
    }
    for (SymbolTable it = thisS; it.hasNext(); ) {
      Object2IntMap.Entry<String> cursor = it.next();
      if (thatS.contains(cursor.getKey())) {
        int thatV = thatS.get(cursor.getKey());
        if (thatV == cursor.getIntValue()) {
          continue;
        }
        reporter.report("symbolTable.key", cursor.getIntValue(), thatV);
      } else {
        reporter.report("symbolTable.missingKey", cursor.getKey(), cursor.getIntValue());
      }
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
