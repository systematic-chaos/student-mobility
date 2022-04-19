/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 11: Gender proportion of Spanish exchange students.
 * 
 * spark - GeneralQuery11.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.col;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery11 {
	
	public static class SpanishGenderProportion implements SparkTask<Double> {
		
		private static final String SPAIN = "ES";
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery11.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> genderProportion = new ArrayList<>(2);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				Dataset<Student> spanishStudents = students.filter(col("countryOfHomeInstitution").equalTo(SPAIN));
				final double spanishStudentCount = (double) spanishStudents.count();
				Dataset<Row> spanishStudentsGender = spanishStudents.groupBy(col("gender")).count();
				
				spanishStudentsGender.orderBy(col("gender")).takeAsList(2).forEach(
						s -> genderProportion.add(new KeyCountPair<>(
								String.valueOf(s.getString(0).charAt(0)),
								roundHalfDown(s.getLong(1) / spanishStudentCount))));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return genderProportion;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery11 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark11-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new SpanishGenderProportion(), args);
	}
}
