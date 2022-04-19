/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 11: Gender proportion of Spanish exchange students.
 * 
 * hadoop - GeneralQuery11.java
 */

package systematicchaos.studentmobility;

import java.util.Iterator;

import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery11 {
	
	public static class SpanishStudentGenderCountMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private static final Text GENDER_MALE = new Text(String.valueOf(Student.Gender.MALE.getValue()));
		private static final Text GENDER_FEMALE = new Text(String.valueOf(Student.Gender.FEMALE.getValue()));
		private static final DoubleWritable ONE = new DoubleWritable(1.);
		
		private static final String SPAIN = "ES";
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String token) {
			KeyValuePair<Text, DoubleWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				if (SPAIN.equals(student.getCountryOfHomeInstitution())) {
					if (Student.Gender.MALE == student.getGender()) {
						mapResult = new KeyValuePair<>(GENDER_MALE, ONE);
					} else if (Student.Gender.FEMALE == student.getGender()) {
						mapResult = new KeyValuePair<>(GENDER_FEMALE, ONE);
					}
				}
			}
			return mapResult;
		}
	}
	
	public static class SpanishStudentGenderProportionReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private DoubleWritable genderProportion = new DoubleWritable();
		
		private static long maleCount = 0;
		private static long femaleCount = 0;
		private static long numValues = 0;
		
		private static final String GENDER_MALE_VALUE = String.valueOf(Student.Gender.MALE.getValue());
		private static final String GENDER_FEMALE_VALUE = String.valueOf(Student.Gender.FEMALE.getValue());
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			long genderCount = 0;
			while (values.hasNext()) {
				genderCount += values.next().get();
				numValues++;
			}
			
			final String keyGender = new String(key.getBytes(), StandardCharsets.UTF_8);
			if (GENDER_MALE_VALUE.equals(keyGender)) {
				maleCount += genderCount;
				genderCount = maleCount;
			} else if (GENDER_FEMALE_VALUE.equals(keyGender)) {
				femaleCount += genderCount;
				genderCount = femaleCount;
			}
			genderProportion.set(roundHalfDown((double)genderCount / numValues));
			return genderProportion;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery11 ../data/Student_Mobility.csv out/hadoop11-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery11.class.getName(),
				SpanishStudentGenderCountMapper.class, SpanishStudentGenderProportionReducer.class,
				Text.class, DoubleWritable.class, args);
		System.exit(exitCode);
	}
}
