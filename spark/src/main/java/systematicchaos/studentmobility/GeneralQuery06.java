/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 6: Destination country preferred by each country's students.
 * 
 * spark - GeneralQuery06.java
 */

package systematicchaos.studentmobility;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class GeneralQuery06 {

	public static class StudentsFavoriteDestinationCountry implements SparkTask<Integer> {
		
		@Override
		public Iterable<KeyCountPair<Integer>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery06.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Integer>> destinationCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				students.sqlContext().udf().register("HOST_COUNTRY",
						(UDF3<String, String, String, String>)(mobilityType, countryHostInstitution, countryWorkPlacement) -> {
							String hostCountry = null;
							if (mobilityType != null && !mobilityType.isEmpty()) {
								if (Student.MobilityType.STUDY.getValue() == mobilityType.charAt(0)) {
									hostCountry = countryHostInstitution;
								} else if (Student.MobilityType.PLACEMENT.getValue() == mobilityType.charAt(0)) {
									hostCountry = countryWorkPlacement;
								}
							}
							return hostCountry;
						}, DataTypes.StringType);
				
				Column hostCountryCol = new Column("hostCountry");
				Dataset<Row> hostCountryStudents = students.withColumn("hostCountry", functions.callUDF("HOST_COUNTRY",
						functions.col("mobilityType"), functions.col("countryOfHostInstitution"), functions.col("countryOfWorkPlacement")))
						.filter(hostCountryCol.notEqual(""))
						.groupBy(new Column("countryOfHomeInstitution"), hostCountryCol).count();
				WindowSpec homeCountryWindow = Window.partitionBy("countryOfHomeInstitution");
				hostCountryStudents = hostCountryStudents.withColumn("maxCount", functions.max(functions.col("count")).over(homeCountryWindow))
						.where(functions.col("count").equalTo(functions.col("maxCount")))
						.drop("maxCount")
						.sort("countryOfHomeInstitution");
				
				int nrows = (int) hostCountryStudents.count();
				destinationCount = hostCountryStudents.takeAsList(nrows).stream()
						.map((s) -> new KeyCountPair<>(String.format("%s\t%s", s.getString(0), s.getString(1)), (int)s.getLong(2)))
						.collect(Collectors.toList());
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return destinationCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery06 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark06-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudentsFavoriteDestinationCountry(), args);
	}
}
