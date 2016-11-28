package hadooputil;

import utiltools.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HadoopMapper extends Mapper<LongWritable, Text, IntWritable, Text> 
{
	private static final BoundaryHelper bh = BoundaryHelper.bound;
	private final HashMap<SpaceIndexObj, AtomicInteger> map = new HashMap<>();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException 
	{
		//BoundaryHelper bh = bh.bound;
		//DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String inputLine = value.toString();
        
        String[] stringArray = inputLine.split("\\,");
        Text outputText = new Text();
        
		if (true) 
		{
			String dateStr = (stringArray[1].split(" "))[0];
			String longStr = stringArray[6];
			String latStr = stringArray[7];
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
						map.get(spaceCell).incrementAndGet();

					} 
					else 
					{
						map.put(spaceCell, new AtomicInteger(1));
					}
				}
			} 
			catch (Exception e) 
			{
				// do nothing
			}
		}
		
		for (Entry<SpaceIndexObj, AtomicInteger> entry : map.entrySet()) 
		{
			SpaceIndexObj spaceCell = entry.getKey();
			System.out.println("" + spaceCell.x + "," + spaceCell.y + "," + spaceCell.t + "," + entry.getValue().intValue());
			outputText.set("" + spaceCell.x + "," + spaceCell.y + "," + spaceCell.t + "," + entry.getValue().intValue());
			context.write(null, outputText);
		}
	}
}
