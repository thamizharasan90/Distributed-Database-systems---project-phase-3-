package utiltools;

public class SpaceIndexObj {
	public static int xSize;
	public static int ySize;
	public static int tSize;
			
	public int x;
	public int y;
	public int t;
	
	public SpaceIndexObj(int x, int y, int t) {
		this.x = x;
		this.y = y;
		this.t = t;
	}
	@Override
	public boolean equals(Object obj) {
		
		if ( this == obj ) {
			return true;
		}
		
		if (!(obj instanceof SpaceIndexObj)) {
			return false;
		}
		
		SpaceIndexObj spc = (SpaceIndexObj)obj;
		if (spc.x == this.x && spc.y == this.y && spc.t == this.t) {
			return true;
		}
		
		return false;
	}
	
	@Override
	public int hashCode() {
		return t * ySize * xSize + y * xSize + x;
	}
	
	@Override
	public String toString() {
		return "SpaceTimeCoordinate(x:" + this.x + ",y:" + this.y + ",t:" + this.t + ")";
	}

}
