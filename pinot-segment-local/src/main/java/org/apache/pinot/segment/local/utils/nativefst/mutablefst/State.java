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

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.util.List;


/**
 * Abstract interface of a State in an FST.
 * @author Atri Sharma
 */
public interface State {

  /**
   * The id for this state; ids are internal and can change through transformations. If you need some stable labeling
   * of states then use state labels (i.e. a state symbol table)
   * @return
   */
  int getId();

  /**
   * Is the state the last state for a path?
   */
  boolean isTerminal();

  /**
   * The outgoing arc count in this state (including self-loops)
   * @return
   */
  int getArcCount();

  /**
   * Get's the ith outgoing arc for this state
   * @param index
   * @return
   */
  Arc getArc(int index);

  /**
   * Get's the entire list of outgoing arcs for this state
   * @return
   */
  List<? extends Arc> getArcs();
}
