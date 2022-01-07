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

import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.FstUtils;


/**
 * A mutable FST's arc implementation.
 */
public class MutableArc implements Arc {

  private int oLabel;
  private String outputSymbol;
  private MutableState nextState;

  /**
   * Arc Constructor
   *
   * @param oLabel    the output label's id
   * @param nextState the arc's next state
   */
  public MutableArc(int oLabel, String outputSymbol, MutableState nextState) {
    this.oLabel = oLabel;
    this.outputSymbol = outputSymbol;
    this.nextState = nextState;
  }

  /**
   * Get the output label's id
   */
  @Override
  public int getOlabel() {
    return oLabel;
  }

  @Override
  public String getOutputSymbol() {
    return outputSymbol;
  }

  /**
   * Set the output label's id
   *
   * @param oLabel the output label's id to set
   */
  public void setOlabel(int oLabel) {
    this.oLabel = oLabel;
  }

  /**
   * Get the next state
   */
  @Override
  public MutableState getNextState() {
    return nextState;
  }

  /**
   * Set the next state
   *
   * @param nextState the next state to set
   */
  public void setNextState(MutableState nextState) {
    this.nextState = nextState;
  }

  @Override
  public boolean equals(Object obj) {
    return FstUtils.arcEquals(this, obj);
  }

  @Override
  public int hashCode() {
    int result = 1;

    result = 31 * result + oLabel;
    result = 31 * result + (nextState != null ? nextState.getId() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "(" + oLabel + ", " + nextState
           + ")";
  }
}
