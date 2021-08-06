package org.apache.pinot.fsa;

/**
 * State visitor.
 * 
 * @see FSA#visitInPostOrder(StateVisitor)
 * @see FSA#visitInPreOrder(StateVisitor)
 */
public interface StateVisitor {
    boolean accept(int state);
}