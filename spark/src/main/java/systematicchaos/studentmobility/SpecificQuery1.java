/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * Specific query 1: How many students, from the same home country as the 22-aged individual,
 * whose home university code is "CIUDA-R" and whose host university code is "S VASTERA",
 * took their Erasmus exchange in the same host university.
 * 
 * spark - SpecificQuery1.java
 */

package systematicchaos.studentmobility;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class SpecificQuery1 {
	
	public static class CountryCompanions implements SparkTask<Long> {
		
		private static final String HOME_UNIVERSITY = "E  CIUDA-R";
		private static final String HOST_UNIVERSITY = "S  VASTERA";
		private static final int AGE = 22;
		
		@Override
		public Iterable<KeyCountPair<Long>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(SpecificQuery1.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Long>> companionCount = new ArrayList<>(1);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				Row me = students.filter(students.col("homeInstitution").startsWith(HOME_UNIVERSITY).and(
						students.col("hostInstitution").startsWith(HOST_UNIVERSITY)).and(
								students.col("age").equalTo(AGE)))
						.select(new Column("countryOfHomeInstitution"), new Column("countryOfHostInstitution"))
						.first();
				
				Dataset<Student> companions = students.filter(students.col("countryOfHomeInstitution").equalTo(me.getString(0))
						.and(students.col("countryOfHostInstitution").equalTo(me.getString(1))));
				companionCount.add(new KeyCountPair<>("COUNTRY_COMPANIONS", companions.count()));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return companionCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.SpecificQuery1 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark-1-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new CountryCompanions(), args);
	}
}
