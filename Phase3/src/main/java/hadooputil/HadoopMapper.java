package hadooputil;

import utiltools.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map.Entry;
//import org.apache.hadoop.io.IntWritable;
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
		//BoundaryHelper bh = bh.bound;
		//DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String inputLine = value.toString();
        
        String[] stringArray = inputLine.split("\\,");
        //Text outputText = new Text();
        //System.out.println("output from mapper "+  stringArray.toString());
        
		if (true) 
		{
			String dateStr = (stringArray[1].split(" "))[0];
			String longStr = stringArray[5];
			String latStr = stringArray[6];
			//System.out.println(dateStr);
			//System.out.println(longStr);
			//System.out.println(latStr);

			try 
			{	
				//System.out.println("first line try " );
				double longt = Double.parseDouble(longStr);
				double latt = Double.parseDouble(latStr);
				//System.out.println("longt longt" + longt +"" + latt);
				int x = bh.getXCell(longt);
				int y = bh.getYCell(latt);
				int t = bh.getDCell(dateStr);
				//System.out.println("try " + x + y + t);

				if (x > -1 && y > -1 && t > -1 && x <= bh.xnoCell && y <= bh.ynoCell && t <= 30) 
				{	
					//System.out.println("if loop ");
					SpaceIndexObj spaceCell = new SpaceIndexObj(x, y, t);
					if (map.containsKey(spaceCell)) 
					{
						map.get(spaceCell).incrementAndGet();
						System.out.println("inside try catch" + spaceCell.t + spaceCell.x + spaceCell.y);
					} 
					else
					{
						map.put(spaceCell, new AtomicInteger(1));
						System.out.println("else loop " + spaceCell.t + spaceCell.x + spaceCell.y );
					}
				}
			} 
			catch (Exception e) 
			{	
				System.out.println("catch output " + e);;
				// do nothing
				
			}
		}
		
		for (Entry<SpaceIndexObj, AtomicInteger> entry : map.entrySet()) 
		{
			SpaceIndexObj spaceCell = entry.getKey();
			System.out.println("X:" + spaceCell.x + ",Y:" +  spaceCell.y + ",D:" + spaceCell.t + ",Value:" + entry.getValue().intValue());
			long ltemp=Long.parseLong(Integer.toString(spaceCell.x) + Integer.toString(spaceCell.y) + Integer.toString(spaceCell.t), 10);
		//	outputText.set("" + spaceCell.x + "," + spaceCell.y + "," + spaceCell.t + "," + entry.getValue().intValue());
			context.write(new LongWritable(ltemp), new Text("" + spaceCell.x + "," + spaceCell.y + "," + spaceCell.t + "," + entry.getValue().intValue()));
		}
	}
}
