package hadooputil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utiltools.*;

public class HadoopReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
	
	@Override
	protected void reduce(LongWritable arg0, Iterable<Text> arg1,
			Reducer<LongWritable, Text, Text, LongWritable>.Context arg2) throws IOException, InterruptedException 
	{
		HashMap<SpaceIndexObj, AtomicInteger> mp = new HashMap<>();
		
		for(Text val : arg1)
		{
			String[] splitStr = val.toString().split("\\,");
			int x = Integer.parseInt(splitStr[0]);
			int y = Integer.parseInt(splitStr[1]);
			int d = Integer.parseInt(splitStr[2]);
			int v = Integer.parseInt(splitStr[3]);
			
			for (int i = -1; i <= +1; ++i) 
			{
				for (int j = -1; j <= +1; ++j) 
				{
					for (int k = -1; k <= +1; ++k) 
					{
						SpaceIndexObj stc = new SpaceIndexObj(x + i, y + j, d + k + 1);
						if (!mp.containsKey(stc)) {
							mp.put(stc, new AtomicInteger(v));
						} else {
							mp.get(stc).addAndGet(v);
						}
					}
				}
			}
		}
		LinkedList<SpatialTuple> list = new LinkedList<>();
		for (Entry<SpaceIndexObj, AtomicInteger> entry : mp.entrySet()) {
			SpaceIndexObj stc = entry.getKey();
			SpatialTuple stvt = new SpatialTuple(stc.x, stc.y, stc.t, entry.getValue().get());
			list.add(stvt);
		}
		
		SpatialTuple[] array = list.toArray(new SpatialTuple[0]);
		Arrays.sort(array);
		
		Text outputText = new Text();
		
		for (int i = 0; i < Math.min(array.length, 50); ++i) 
		{
			outputText.set(array[i].toString());
			System.out.println("reducer value " + outputText);
			arg2.write(outputText, null);
		}
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
	}

}
