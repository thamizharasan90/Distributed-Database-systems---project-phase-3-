package utiltools;


public class BoundaryHelper
{
	public final double slong;
	public final double elong;
	public final double slat;
	public final double elat;
	
	public final double dsizeval;
	public final double ynoCell;
	public final double xnoCell;
	public final double ysize;
	public final double xsize;
	
	
	public final double cellparam;

	
//	private final SimpleDateFormat formatter;
	public static BoundaryHelper bound = new BoundaryHelper(0.01, -74.25, -73.70, 40.5, 40.9);//,"2015-01-01","2015-01-31"
	public BoundaryHelper(double cellparam,  double startlongitude, double endlongitude,double startlatitude,double endlatitude)//, startDate, String endDate
	{
		this.cellparam = cellparam;
		double temp = (long)(startlongitude/cellparam);
		temp = temp * cellparam;
		if(temp > cellparam)
			temp = temp - cellparam;
		this.slong = temp;
		
		this.elong = endlongitude;
		
		this.slat = startlatitude;
		
		temp = (long)(endlatitude/cellparam);
		temp = temp * cellparam;
		if(temp < cellparam)
			temp = temp + cellparam;
		this.elat = temp;
		
		this.ynoCell = (int)((elat - slat)/cellparam) + 1;
		this.xnoCell = (int)((elong - slong)/cellparam) + 1;
		
		
		this.dsizeval = 31;
		
		this.xsize = this.xnoCell + 1;
		this.ysize = this.ynoCell + 1;
		
		
		/**formatter = new SimpleDateFormat("yyyy-MM-dd");
		try
		{
			Date stDate = (Date)formatter.parse(startDate);
			Date enDate = (Date)formatter.parse(endDate);
		}
		catch(Exception e){
			
		}**/
	}
	
	public int getXCell(double longitude) {
		System.out.println("value of slong " + slong );
		System.out.println("value of elong " + elong );
		if (longitude < slong || longitude > elong) {
			return -1;
		}
		return (int)Math.floor((longitude - slong) / cellparam);
	}
	
	public int getYCell(double latitude) {
		System.out.println("value of slat " + slat);
		System.out.println("value of elat " + elat );
		if (latitude < slat || latitude > elat) {
			return -1;
		}
		return (int)Math.floor((latitude - slat) / cellparam);
	}
	
	public int getDCell(String dt) {
		return (int)((Integer.parseInt(dt.split("-")[2])) - 1);
	}
	
	
}
