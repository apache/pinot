package com.linkedin.pinot.core.common;

import java.util.Comparator;

public class Pairs {

	public static IntPair intPair(int a, int b) {
		return new IntPair(a, b);
	}

	public static Comparator<IntPair> intPairComparator() {
		return new AscendingIntPairComparator();
	}

	public static class IntPair{
		int a;

		int b;

		public IntPair(int a, int b) {
			this.a = a;
			this.b = b;
		}

		public int getA() {
			return a;
		}

		public int getB() {
			return b;
		}
	}

	public static class AscendingIntPairComparator implements Comparator<IntPair> {

		@Override
		public int compare(IntPair o1, IntPair o2) {
			return new Integer(o1.a).compareTo(new Integer(o2.a));
		}

	}

}
