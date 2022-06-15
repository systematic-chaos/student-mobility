/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 12: Gender proportion of exchange students received by Spanish host institutions.
 * 
 * spark - GeneralQuery12.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

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
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery12 {
	
	public static class VisitorGenderProportion implements SparkTask<Double> {
		
		private static final String SPAIN = "ES";
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery12.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> genderProportion = new ArrayList<>(2);
			
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
				
				Dataset<Row> visitorStudents = students.withColumn("hostCountry", callUDF("HOST_COUNTRY",
						col("mobilityType"), col("countryOfHostInstitution"), col("countryOfWorkPlacement")))
						.filter(col("hostCountry").equalTo(SPAIN));
				final double visitorStudentCount = (double) visitorStudents.count();
				Dataset<Row> visitorStudentsGender = visitorStudents.groupBy(col("gender")).count();
				
				visitorStudentsGender.orderBy(col("gender")).takeAsList(2).forEach(
						s -> genderProportion.add(new KeyCountPair<>(
								String.valueOf(s.getString(0).charAt(0)),
								roundHalfDown(s.getLong(1) / visitorStudentCount))));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return genderProportion;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery12 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark12-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new VisitorGenderProportion(), args);
	}
}
