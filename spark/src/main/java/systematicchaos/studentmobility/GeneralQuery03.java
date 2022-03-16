/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 3: Proportion of exchange students sent/received per country.
 * 
 * spark - GeneralQuery3.java
 */

package systematicchaos.studentmobility;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery03 {
	
	public static class StudentDeliveryReceptionProportion implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery03.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> studentCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				Dataset<Row> countrySendsStudents = students.groupBy(
						new Column("countryOfHomeInstitution").as("homeCountry"))
						.count().withColumnRenamed("count", "homeCount");
				Dataset<Row> countryReceivesStudents = students.groupBy(
						new Column("countryOfHostInstitution").as("hostCountry"))
						.count().withColumnRenamed("count", "hostCount")
						.filter(functions.col("hostCountry").rlike("[A-Z]+"));
				Dataset<Row> deliveryReceptionProportion = countrySendsStudents.join(countryReceivesStudents,
						functions.col("homeCountry").equalTo(functions.col("hostCountry")), "full_outer")
						.na().fill(0)
						.select(functions.col("hostCountry").as("country"),
								functions.col("homeCount").divide(functions.col("hostCount")).as("count"))
						.sort("country");
				
				int nrows = (int) deliveryReceptionProportion.count();
				studentCount = deliveryReceptionProportion.takeAsList(nrows).stream()
						.map((r) -> new KeyCountPair<>(r.getString(0), r.getDouble(1)))
						.collect(Collectors.toList());
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return studentCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery03 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark03-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudentDeliveryReceptionProportion(), args);
	}
}
