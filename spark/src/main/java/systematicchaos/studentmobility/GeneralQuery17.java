/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 17: Average economic grant per European exchange student.
 * 
 * spark - GeneralQuery17.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
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

public class GeneralQuery17 {
	
	private static final String EUROPE = "EU";
	
	public static class EuropeanEconomicalGrant implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery17.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> economicalGrant = new ArrayList<>(1);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				students.sqlContext().udf().register("ECONOMICAL_GRANT",
						(UDF3<String, Float, Float, Float>)(mobilityType, studyGrant, placementGrant) -> {
							Float grant = null;
							char mt = mobilityType.charAt(0);
							if (Student.MobilityType.STUDY.getValue() == mt) {
								grant = studyGrant != null ? studyGrant : 0f;
							} else if (Student.MobilityType.PLACEMENT.getValue() == mt) {
								grant = placementGrant != null ? placementGrant : 0f;
							}
							return grant;
						}, DataTypes.FloatType);
				
				Column grantCol = new Column("grant");
				Dataset<Row> studentsGrant = students
						.withColumn("grant", callUDF("ECONOMICAL_GRANT",
								col("MOBILITYTYPE"), col("STUDYGRANT"), col("PLACEMENTGRANT")))
						.filter(grantCol.isNotNull());
				
				double grant = roundHalfDown(studentsGrant.select(mean(grantCol)).first().getDouble(0), 2);
				economicalGrant.add(new KeyCountPair<>(EUROPE, grant));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return economicalGrant;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery17 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark17-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new EuropeanEconomicalGrant(), args);
	}
}
