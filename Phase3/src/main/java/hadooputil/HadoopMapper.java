package hadooputil;

import utiltools.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HadoopMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
{
	private static final BoundaryHelper bh = BoundaryHelper.bound;
	private final HashMap<SpaceIndexObj, AtomicInteger> map = new HashMap<>();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException 
	{
		//System.out.println("**********INSIDE MAPPER*********");
        String inputLine = value.toString();
        String[] stringArray = inputLine.split("\\,");
        //System.out.println("Length of input line :  " + stringArray.length);
		if (true) 
		{
			String dateStr = (stringArray[1].split(" "))[0];
			String longStr = stringArray[5];
			String latStr = stringArray[6];
			try 
			{	
				double longt = Double.parseDouble(longStr);
				double latt = Double.parseDouble(latStr);
				int x = bh.getXCell(longt);
				int y = bh.getYCell(latt);
				int t = bh.getDCell(dateStr);

				if (x > -1 && y > -1 && t > -1 && x <= bh.xnoCell && y <= bh.ynoCell && t <= 30) 
				{	
					SpaceIndexObj spaceCell = new SpaceIndexObj(x, y, t);
					if (map.containsKey(spaceCell)) 
					{
						//System.out.println("inside if exsist part BEFORE:- " + spaceCell.x + "  " + spaceCell.y + "  " + spaceCell.t );
						map.get(spaceCell).incrementAndGet();
						//System.out.println("inside if exsist part AFTER INCREMENTANDGET:- " + spaceCell.x + "  " + spaceCell.y + "  " + spaceCell.t );
					} 
					else 
					{
						//System.out.println("else not exsist part BEFORE :-" + spaceCell.x + "  " + spaceCell.y + "  " + spaceCell.t );
						map.put(spaceCell, new AtomicInteger(1));
						//System.out.println("else not exsist part AFTER :-" + spaceCell.x + "  " + spaceCell.y + "  " + spaceCell.t );
					}
				}
			} 
			catch (Exception e) 
			{	
				System.out.println("catch output " + e);;
			}
		}
		
		for (Entry<SpaceIndexObj, AtomicInteger> entry : map.entrySet()) 
		{
			SpaceIndexObj spaceCell = entry.getKey();
			//System.out.println("X: " + spaceCell.x + ",Y: " +  spaceCell.y + ",D: " + spaceCell.t + ",Value: " + entry.getValue().intValue());
			long ltemp=Long.parseLong(Integer.toString(spaceCell.x) + Integer.toString(spaceCell.y) + Integer.toString(spaceCell.t), 10);
			context.write(new LongWritable(ltemp), new Text("" + spaceCell.x + "," + spaceCell.y + "," + spaceCell.t + "," + entry.getValue().intValue()));
		}
	}
}
