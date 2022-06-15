/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 2: Total exchange students received by each country.
 * 
 * hadoop - GeneralQuery02.java
 */

package systematicchaos.studentmobility;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery02 {
	
	public static class StudentMapper extends HadoopMapper<Text, LongWritable> {
		
		private static final LongWritable ONE = new LongWritable(1l);
		private Text country = new Text();
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				if (student.getCountryOfHostInstitution() != null) {
					country.set(student.getCountryOfHostInstitution());
					mapResult = new KeyValuePair<>(country, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class CountReducer extends HadoopReducer<Text, LongWritable> {
		
		private LongWritable reduceResult = new LongWritable();
		
		@Override
		public LongWritable reduce(Text key, Iterator<LongWritable> values) {
			long count = 0l;
			while (values.hasNext()) {
				count += values.next().get();
			}
			reduceResult.set(count);
			return reduceResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery02 ../data/Student_Mobility.csv out/hadoop02-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery02.class.getName(),
				StudentMapper.class, CountReducer.class,
				Text.class, LongWritable.class, args);
		System.exit(exitCode);
	}
}
