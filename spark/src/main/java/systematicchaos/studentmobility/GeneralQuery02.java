/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 2: Total exchange students received by each country.
 * 
 * spark - GeneralQuery02.java
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

public class GeneralQuery02 {
	
	public static class StudentsReceivedByHostCountry implements SparkTask<Long> {
		
		@Override
		public Iterable<KeyCountPair<Long>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery02.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Long>> studentCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				Dataset<Row> countryReceivesStudents = students.groupBy(
						new Column("countryOfHostInstitution").as("hostCountry"))
						.count().sort("country");
				
				int nrows = (int) countryReceivesStudents.count();
				studentCount = countryReceivesStudents.takeAsList(nrows).stream()
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
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery02 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark02-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudentsReceivedByHostCountry(), args);
	}
}
