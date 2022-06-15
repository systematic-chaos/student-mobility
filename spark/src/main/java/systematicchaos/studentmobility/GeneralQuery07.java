/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 7: Average age of European exchange students.
 * 
 * spark - GeneralQuery07.java
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

public class GeneralQuery07 {
	
	public static class EuropeanAverageAge implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery07.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> europeanAge = new ArrayList<>(1);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				double age = students.select(mean(col("age"))).first().getDouble(0);
				europeanAge.add(new KeyCountPair<>("AGE", Functions.roundHalfDown(age)));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": "+ e.getMessage());
			} finally {
				job.close();
			}
			
			return europeanAge;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery07 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark07-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new EuropeanAverageAge(), args);
	}
}
