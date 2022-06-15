/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 14: Proportion, of those students that initially stay abroad just for the
 * fall semester, that extend their exchange period to the next spring semester.
 * 
 * hadoop - GeneralQuery14.java
 */

package systematicchaos.studentmobility;

import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery14 {
	
	private static final String YES_VALUE = String.valueOf('Y');
	private static final String NO_VALUE = String.valueOf('N');
	
	public static class PlacementExtensionMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private static final Text YES = new Text(YES_VALUE);
		private static final Text NO = new Text(NO_VALUE);
		private static final DoubleWritable ONE = new DoubleWritable(1.);
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String token) {
			KeyValuePair<Text, DoubleWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				boolean placementExtension = getPlacementExtension(student);
				mapResult = new KeyValuePair<>(placementExtension ? YES : NO, ONE);
			}
			return mapResult;
		}
		
		private static boolean getPlacementExtension(Student student) {
			LocalDate startDate = getStudyPlacementStartDate(student);
			Float length = getStudyPlacementLength(student);
			return length != null && length >= 7f
					&& startDate != null && startDate.getMonthValue() > 6;
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
	}
	
	public static class PlacementExtensionProportionReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private DoubleWritable placementExtensionProportion = new DoubleWritable();
		
		private static long yesCount = 0;
		private static long noCount = 0;
		private static long numValues = 0;
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			long extensionCount = 0;
			while (values.hasNext()) {
				extensionCount += values.next().get();
				numValues++;
			}
			
			final String keyExtension = new String(key.getBytes(), StandardCharsets.UTF_8);
			if (YES_VALUE.equals(keyExtension)) {
				yesCount += extensionCount;
				extensionCount = yesCount;
			} else if (NO_VALUE.equals(keyExtension)) {
				noCount += extensionCount;
				extensionCount = noCount;
			}
			placementExtensionProportion.set(roundHalfDown((double)extensionCount / numValues));
			return placementExtensionProportion;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery14 ../data/Student_Mobility.csv out/hadoop14-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery14.class.getName(),
				PlacementExtensionMapper.class, PlacementExtensionProportionReducer.class,
				Text.class, DoubleWritable.class, args);
		System.exit(exitCode);
	}
}
