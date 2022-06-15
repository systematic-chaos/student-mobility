/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 17: Average economic grant per European exchange student.
 * 
 * hadoop - GeneralQuery17.java
 */

package systematicchaos.studentmobility;

import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery17 {
	
	public static class EuropeanEconomicalGrantMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private static final String HEADER = "HOMEINSTITUTION";
		private static String EUROPE = "EU";
		
		private static final Text COUNTRY = new Text(EUROPE);
		private DoubleWritable economicalGrant = new DoubleWritable();
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String token) {
			KeyValuePair<Text, DoubleWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				
				Student.MobilityType mobilityType = student.getMobilityType();
				Float grant = null;
				
				if (Student.MobilityType.STUDY == mobilityType || Student.MobilityType.PLACEMENT == mobilityType) {
					grant = Student.MobilityType.STUDY == mobilityType ? student.getStudyGrant() : student.getPlacementGrant();
					if (grant == null) grant = 0f;
				}
				
				if (grant != null) {
					economicalGrant.set(grant.doubleValue());
					mapResult = new KeyValuePair<>(COUNTRY, economicalGrant);
				}
			}
			return mapResult;
		}
	}
	
	public static class EuropeanEconomicalGrantReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private static double economicalGrantSum = 0.;
		private static long numStudents = 0;
		
		private DoubleWritable reduceResult = new DoubleWritable();
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			while (values.hasNext()) {
				economicalGrantSum += values.next().get();
				numStudents++;
			}
			reduceResult.set(roundHalfDown(economicalGrantSum / numStudents, 2));
			return reduceResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery17 ../data/Student_Mobility.csv out/hadoop17-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(GeneralQuery17.class.getName(),
				EuropeanEconomicalGrantMapper.class, EuropeanEconomicalGrantReducer.class,
				Text.class, DoubleWritable.class, args);
		System.exit(exitCode);
	}
}
