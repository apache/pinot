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

/**
 * Interface for the contract of a mutable Arc in an FST
 */
public interface Arc {

  /**
   * Get the index of the input symbol for this edge of the fst
   * @return
   */
  int getIlabel();

  /**
   * Get the index of the output symbol for this edge of the fst
   * @return
   */
  int getOlabel();

  /**
   * Get the reference to the next state in the FST; note that you get call `getNextState().getId()` to get the
   * FST state id for that state but some operations will be constructing new results and state ids will not be
   * consistent across them (obviously). If you are using state symbols/labels then the labels will be constistent
   * @return
   */
  State getNextState();
}
