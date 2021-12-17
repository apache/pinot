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

  private double weight;
  private int iLabel;
  private int oLabel;
  private MutableState nextState;

  /**
   * Arc Constructor
   *
   * @param iLabel    the input label's id
   * @param oLabel    the output label's id
   * @param weight    the arc's weight
   * @param nextState the arc's next state
   */
  public MutableArc(int iLabel, int oLabel, double weight, MutableState nextState) {
    this.weight = weight;
    this.iLabel = iLabel;
    this.oLabel = oLabel;
    this.nextState = nextState;
  }

  /**
   * Get the input label's id
   */
  @Override
  public int getIlabel() {
    return iLabel;
  }

  /**
   * Set the input label's id
   *
   * @param iLabel the input label's id to set
   */
  public void setIlabel(int iLabel) {
    this.iLabel = iLabel;
  }

  /**
   * Get the output label's id
   */
  @Override
  public int getOlabel() {
    return oLabel;
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
    int result;
    long temp;
    temp = Double.doubleToLongBits(weight);
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + iLabel;
    result = 31 * result + oLabel;
    result = 31 * result + (nextState != null ? nextState.getId() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "(" + iLabel + ", " + oLabel + ", " + weight + ", " + nextState
           + ")";
  }
}
