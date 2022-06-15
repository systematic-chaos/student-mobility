/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 19: Distribution of exchange students in terms of their occupation
 * (study or internship/placement) and level (graduate or postgraduate).
 * Therefore, four groups are considered: graduate students, graduate interns,
 * postgraduate students and postgraduate interns.
 * 
 * spark - GeneralQuery19.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.sum;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery19 {
	
	public static class MobilityTypeStudyLevelDistribution implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery19.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> mobilityTypeStudyLevel = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				students = students.filter(new Column("mobilityType").isNotNull())
						.filter(new Column("studyLevel").isNotNull());
				
				Dataset<Row> mltsGroups = students.groupBy("mobilityType", "studyLevel").count()
						.orderBy("mobilityType", "studyLevel");
				long numStudents = mltsGroups.agg(sum("count")).first().getLong(0);
				
				mltsGroups.sqlContext().udf().register("MLTS_PROPORTION",
						(UDF1<Long, Double>)(count) -> {
							return (double) count / numStudents;
						}, DataTypes.DoubleType);
				mltsGroups = mltsGroups.withColumn("proportion", callUDF("MLTS_PROPORTION", new Column("count")));
				
				mobilityTypeStudyLevel = mltsGroups.takeAsList((int) mltsGroups.count()).stream()
						.map((mlts) -> new KeyCountPair<>(
								String.format("%s\t%s", Student.MobilityType.valueOf(mlts.getString(0)).getValue(),
										Student.StudyLevel.valueOf(mlts.getString(1)).getValue()),
								roundHalfDown(mlts.getDouble(3))))
						.collect(Collectors.toList());
			} catch (Exception e) {
				System.err.println(e.getClass().getName()+ ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return mobilityTypeStudyLevel;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery19 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark19-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new MobilityTypeStudyLevelDistribution(), args);
	}
}
