package com.linkedin.thirdeye.bootstrap.aggregation;

public enum MetricType {

	INT {
		public Number toNumber(String s) {
			return Integer.parseInt(s);
		}

		public int byteSize() {
			return 4;
		}

	},
	SHORT {
		public Number toNumber(String s) {
			return Integer.parseInt(s);
		}

		public int byteSize() {
			return 2;

		}

	},
	LONG {
		public Number toNumber(String s) {
			return Integer.parseInt(s);
		}

		public int byteSize() {
			return 8;

		}

	},
	FLOAT {
		public Number toNumber(String s) {
			return Integer.parseInt(s);
		}

		public int byteSize() {
			return 4;

		}

	},
	DOUBLE {
		public Number toNumber(String s) {
			return Integer.parseInt(s);
		}

		public int byteSize() {
			return 8;
		}
	};

	public Number toNumber(String s) {
		throw new AbstractMethodError();
	}

	public int byteSize() {
		throw new AbstractMethodError();
	}
}
