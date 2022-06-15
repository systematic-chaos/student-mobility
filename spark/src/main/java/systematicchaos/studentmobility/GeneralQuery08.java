/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 8: Average age of Spanish exchange students.
 * 
 * spark - GeneralQuery08.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.Functions;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery08 {
	
	public static class SpanishAverageAge implements SparkTask<Double> {
		
		private static final String SPAIN = "ES"; 
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery08.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> spanishAge = new ArrayList<>(1);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				double age = students.filter(col("countryOfHomeInstitution").equalTo(SPAIN))
						.select(mean(col("age"))).first().getDouble(0);
				spanishAge.add(new KeyCountPair<>("AGE", Functions.roundHalfDown(age)));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return spanishAge;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery08 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark08-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new SpanishAverageAge(), args);
	}
}
