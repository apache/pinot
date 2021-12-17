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

public class IndexPair {

  private final int left;
  private final int right;

  public IndexPair(int left, int right) {
    this.left = left;
    this.right = right;
  }

  public int getLeft() {
    return left;
  }

  public int getRight() {
    return right;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IndexPair indexPair = (IndexPair) o;

    if (left != indexPair.left) {
      return false;
    }
    return right == indexPair.right;

  }

  @Override
  public int hashCode() {
    int result = left;
    result = 31 * result + right;
    return result;
  }

  @Override
  public String toString() {
    return "IndexPair{" +
           "left=" + left +
           ", right=" + right +
           '}';
  }
}
