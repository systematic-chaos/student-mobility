/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 7: Average age of European exchange students.
 * 
 * hadoop - GeneralQuery07.java
 */

package systematicchaos.studentmobility;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.Functions;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery07 {
	
	public static class StudentAgeMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private static final Text AGE_FIELD = new Text("AGE");
		private DoubleWritable age = new DoubleWritable();
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String token) {
			KeyValuePair<Text, DoubleWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Integer studentAge = Student.fromString(token).getAge();
				if (studentAge != null && studentAge > 0) {
					age.set(studentAge.doubleValue());
					mapResult = new KeyValuePair<>(AGE_FIELD, age);
				}
			}
			return mapResult;
		}
	}
	
	public static class StudentAgeReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private DoubleWritable reduceResult = new DoubleWritable();
		
		private static double sum = 0.;
		private static long numValues = 0;
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			while (values.hasNext()) {
				sum += values.next().get();
				numValues++;
			}
			reduceResult.set(Functions.roundHalfDown(sum / numValues));
			return reduceResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery07 ../data/Student_Mobility.csv out/hadoop07-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery07.class.getName(),
				StudentAgeMapper.class, StudentAgeReducer.class,
				Text.class, DoubleWritable.class, args);
		System.exit(exitCode);
	}
}
