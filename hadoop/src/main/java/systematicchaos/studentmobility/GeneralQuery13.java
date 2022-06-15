/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 13: Total of exchange students that stay abroad for the fall semester,
 * the spring semester, and the whole academic year, respectively.
 * 
 * hadoop - GeneralQuery13.java
 */

package systematicchaos.studentmobility;

import java.time.LocalDate;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery13 {
	
	public static class StudyPlacementLengthMapper extends HadoopMapper<Text, LongWritable> {
	
		private Text studyPlacementLength = new Text();
		private static final LongWritable ONE = new LongWritable(1l);
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				String period = getPeriodLength(student);
				if (period != null) {
					studyPlacementLength.set(period);
					mapResult = new KeyValuePair<>(studyPlacementLength, ONE);
				}
			}
			return mapResult;
		}
		
		private static String getPeriodLength(Student student) {
			LocalDate startDate = getStudyPlacementStartDate(student);
			Float length = getStudyPlacementLength(student);
			PeriodLength periodLength = null;
			if (length != null && length >= 7f) {
				periodLength = PeriodLength.FULL;
			} else if (startDate != null) {
				if (startDate.getMonthValue() > 6) {
					periodLength = length != null && length > 0f ? PeriodLength.FIRST : PeriodLength.FULL;
				} else {
					periodLength = PeriodLength.SECOND;
				}
			}
			return periodLength != null ? String.valueOf(periodLength.getValue()) : null;
		}
		
		private static LocalDate getStudyPlacementStartDate(Student student) {
			LocalDate startDate;
			switch (student.getMobilityType()) {
			case STUDY:
				startDate = student.getStudyStartDate();
				break;
			case PLACEMENT:
				startDate = student.getPlacementStartDate();
				break;
			default:
				startDate = null;
			}
			return startDate;
		}
		
		private static Float getStudyPlacementLength(Student student) {
			Float length;
			switch (student.getMobilityType()) {
			case STUDY:
				length = student.getLengthStudyPeriod();
				break;
			case PLACEMENT:
				length = student.getLengthWorkPlacement();
				break;
			default:
				length = null;
			}
			return length;
		}
		
		static enum PeriodLength {
			FIRST('1'),
			SECOND('2'),
			FULL('F');
			
			private char value;
			
			private PeriodLength(char value) {
				this.value = value;
			}
			
			public char getValue() {
				return this.value;
			}
			
			@Override
			public String toString() {
				return this.name();
			}
		}
	}
	
	public static class StudyPlacementLengthCountReducer extends HadoopReducer<Text, LongWritable> {
		
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
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery13 ../data/Student_Mobility.csv out/hadoop13-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery13.class.getName(),
				StudyPlacementLengthMapper.class, StudyPlacementLengthCountReducer.class,
				Text.class, LongWritable.class, args);
		System.exit(exitCode);
	}
}
