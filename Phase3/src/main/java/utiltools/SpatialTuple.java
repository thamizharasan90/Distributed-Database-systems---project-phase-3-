package utiltools;

public class SpatialTuple implements Comparable<SpatialTuple>{

		
		public int x;
		public int y;
		public int t;
		public int v;
		
		public SpatialTuple(int x, int y, int t, int v) {
			this.x = x;
			this.y = y;
			this.t = t;
			this.v = v;
		}

		@Override
		public int compareTo(SpatialTuple o) {
			return o.v - this.v;
		}
		
		@Override
		public String toString() {
			return "SpaceTimeValueTuple(x:" + x + ",y:" + y + ",t:" + t +",v:" + v + ")";
		}
		
}
