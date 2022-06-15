/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 4: Total exchange students sent by each Spanish institution.
 * 
 * spark - GeneralQuery04.java
 */

package systematicchaos.studentmobility;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery04 {
	
	public static class StudentsSentBySpanishUniversity implements SparkTask<Integer> {
		
		private static final Pattern UNI_PATTERN = Pattern.compile("([A-Z-]+)([0-9]+)");
		
		private static final String SPAIN = "ES";
		private static final String SPANISH_UNIVERSITY_PREFIX = "E";
		
		@Override
		public Iterable<KeyCountPair<Integer>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery04.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Integer>> studentCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				Dataset<Student> spanishStudents = students.filter(
					students.col("countryOfHomeInstitution").equalTo(SPAIN)
					.and(students.col("homeInstitution").startsWith(SPANISH_UNIVERSITY_PREFIX)));
				
				spanishStudents.sqlContext().udf().register("SPANISH_UNIVERSITY_STUDENT",
					(UDF1<String, String>)(uni) -> {
						Matcher uniMatcher = UNI_PATTERN.matcher(uni.substring(2));
						return uniMatcher.group(uniMatcher.find() ? 1 : 0);
					}, DataTypes.StringType);
				
				Dataset<Row> spanishUniversityStudents = spanishStudents.withColumn("spanishInstitution",
						functions.callUDF("SPANISH_UNIVERSITY_STUDENT", functions.col("homeInstitution")));
				spanishUniversityStudents = spanishUniversityStudents.groupBy(
						new Column("spanishInstitution")).count().sort("spanishInstitution");
				
				int nrows = (int) spanishUniversityStudents.count();
				studentCount = spanishUniversityStudents.takeAsList(nrows).stream()
						.map((r) -> new KeyCountPair<>(r.getString(0), (int)r.getLong(1)))
						.collect(Collectors.toList());
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return studentCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery04 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark04-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudentsSentBySpanishUniversity(), args);
	}
}
