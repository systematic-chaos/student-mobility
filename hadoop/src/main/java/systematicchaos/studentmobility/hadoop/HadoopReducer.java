/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopReducer.java
 */

package systematicchaos.studentmobility.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class HadoopReducer<Key extends BinaryComparable, Value extends Writable>
		extends Reducer<Key, Value, Key, Value> {
	
	@Override
	public void reduce(Key key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {
		Value reducedValue = reduce(key, values.iterator());
		context.write(key, reducedValue);
	}
	
	public abstract Value reduce(Key key, Iterator<Value> values);
}
