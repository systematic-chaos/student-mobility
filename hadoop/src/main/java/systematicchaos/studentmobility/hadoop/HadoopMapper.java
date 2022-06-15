/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopMapper.java
 */

package systematicchaos.studentmobility.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import systematicchaos.studentmobility.util.KeyValuePair;

public abstract class HadoopMapper<OutputKey extends BinaryComparable, OutputValue extends Writable>
		extends Mapper<Object, Text, OutputKey, OutputValue> {
	
	private static final String NEWLINE = "\n";
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer lineItr = new StringTokenizer(value.toString(), NEWLINE);
		while (lineItr.hasMoreTokens()) {
			String line = lineItr.nextToken();
			KeyValuePair<OutputKey, OutputValue> mappedToken = map(line);
			if (mappedToken != null) {
				context.write(mappedToken.getKey(), mappedToken.getValue());
			}
		}
	}
	
	public abstract KeyValuePair<OutputKey, OutputValue> map(String token);
}
