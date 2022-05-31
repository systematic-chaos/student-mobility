/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * Specific query 2: How many students, from the university that the campus with code "CIUDA"
 * belongs to, took their Erasmus exchange in the host university that the campus with code
 * "VASTERA" belongs to. A reference student is provided in order to correlate those campuses
 * to their corresponding universities.
 * 
 * spark - SpecificQuery2.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

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

public class SpecificQuery2 {
	
	public static class UniversityCompanions implements SparkTask<Long> {
		
		private static final String HOME_UNIVERSITY = "CIUDA";
		private static final String HOST_UNIVERSITY = "VASTERA";
		private static final int AGE = 22;
		
		@Override
		public Iterable<KeyCountPair<Long>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(SpecificQuery2.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Long>> companionCount = new ArrayList<>(1);
			
			try {				
				Dataset<Student> students = job.readDataset(inputFiles);
				
				students.sqlContext().udf().register("UNI_SUB_STR",
					(UDF1<String, String>)(uni) -> {
						return uni.length() >= 10 ? uni.substring(3, 10) : uni;
					}, DataTypes.StringType);
				
				Dataset<Row> uniStudents = students
						.withColumn("HOME_INSTITUTION", callUDF("UNI_SUB_STR", col("homeInstitution")))
						.withColumn("HOST_INSTITUTION", callUDF("UNI_SUB_STR", col("hostInstitution")));
				
				Row me = uniStudents.filter(col("HOME_INSTITUTION").startsWith(HOME_UNIVERSITY).and(
						col("HOST_INSTITUTION").startsWith(HOST_UNIVERSITY)).and(
								col("age").equalTo(AGE)))
						.select(col("HOME_INSTITUTION"), col("HOST_INSTITUTION"))
						.first();
				
				Dataset<Row> companions = uniStudents.filter(col("HOME_INSTITUTION").equalTo(me.getString(0))
						.and(col("HOST_INSTITUTION").equalTo(me.getString(1))));
				companionCount.add(new KeyCountPair<>("UNIVERSITY_COMPANIONS", companions.count()));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return companionCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.SpecificQuery2 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark-2-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new UniversityCompanions(), args);
	}
}
