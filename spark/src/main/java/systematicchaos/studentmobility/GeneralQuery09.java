/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 9: Average age of exchange students received by Spanish host institutions.
 * 
 * spark - GeneralQuery09.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.Functions;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery09 {
	
	public static class GuiriAverageAge implements SparkTask<Double> {
		
		private static final String SPAIN = "ES";
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery09.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> guiriAge = new ArrayList<>(1);
			
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
				
				Dataset<Row> guiriStudents = students.withColumn("hostCountry", callUDF("HOST_COUNTRY",
						col("mobilityType"), col("countryOfHostInstitution"), col("countryOfWorkPlacement")))
						.filter(col("hostCountry").equalTo(SPAIN));
				double age = guiriStudents.select(mean(col("age"))).first().getDouble(0);
				guiriAge.add(new KeyCountPair<>("AGE", Functions.roundHalfDown(age)));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return guiriAge;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery09 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark09-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new GuiriAverageAge(), args);
	}
}
