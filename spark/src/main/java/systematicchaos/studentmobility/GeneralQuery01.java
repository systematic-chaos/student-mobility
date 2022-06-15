/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 1: Total exchange students sent by each country.
 * 
 * spark - GeneralQuery01.java
 */

package systematicchaos.studentmobility;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery01 {
	
	public static class StudentsSentByHomeCountry implements SparkTask<Long> {
		
		@Override
		public Iterable<KeyCountPair<Long>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery01.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Long>> studentCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				Dataset<Row> countrySendsStudents = students.groupBy(
						new Column("countryOfHomeInstitution").as("homeCountry"))
						.count().sort("country");
				
				int nrows = (int) countrySendsStudents.count();
				studentCount = countrySendsStudents.takeAsList(nrows).stream()
						.map((r) -> new KeyCountPair<>(r.getString(0), r.getLong(1)))
						.collect(Collectors.toList());
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return studentCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery01 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark01-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudentsSentByHomeCountry(), args);
	}
}
