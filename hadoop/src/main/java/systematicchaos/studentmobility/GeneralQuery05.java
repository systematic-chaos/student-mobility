/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 5: Total exchange students received by each Spanish institution.
 * 
 * hadoop - GeneralQuery05.java
 */

package systematicchaos.studentmobility;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery05 {
	
	public static class StudentAtSpainMapper extends HadoopMapper<Text, IntWritable> {
		
		private Text university = new Text();
		private static final IntWritable ONE = new IntWritable(1);
		private static final Pattern UNI_PATTERN = Pattern.compile("([A-Z-]+)([0-9]+)");
		
		private static final String HEADER = "HOMEINSTITUTION";
		private static final String SPAIN = "ES";
		private static final String SPANISH_UNIVERSITY_PREFIX = "E";
		
		@Override
		public KeyValuePair<Text, IntWritable> map(String token) {
			KeyValuePair<Text, IntWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				if (SPAIN.equals(student.getCountryOfHostInstitution())
						&& student.getHostInstitution().startsWith(SPANISH_UNIVERSITY_PREFIX)) {
					String uni = student.getHostInstitution().substring(1).trim();
					Matcher uniMatcher = UNI_PATTERN.matcher(uni);
					university.set(uniMatcher.find() ? uniMatcher.group(1) : uni);
					mapResult = new KeyValuePair<>(university, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class CountReducer extends HadoopReducer<Text, IntWritable> {
		
		private IntWritable reduceResult = new IntWritable();
		
		@Override
		public IntWritable reduce(Text key, Iterator<IntWritable> values) {
			int count = 0;
			while (values.hasNext()) {
				count += values.next().get();
			}
			reduceResult.set(count);
			return reduceResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery05 ../data/Student_Mobility.csv out/hadoop05-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery05.class.getName(),
				StudentAtSpainMapper.class, CountReducer.class,
				Text.class, IntWritable.class, args);
		System.exit(exitCode);
	}
}
