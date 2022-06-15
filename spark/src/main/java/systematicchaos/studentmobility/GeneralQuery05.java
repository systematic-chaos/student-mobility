/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 5: Total exchange students received by each Spanish institution.
 * 
 * spark - GeneralQuery05.java
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

public class GeneralQuery05 {
	
	public static class StudentsReceivedBySpanishUniversity implements SparkTask<Integer> {
		
		private static final Pattern UNI_PATTERN = Pattern.compile("([A-Z-]+)([0-9]+)");
		
		private static final String SPAIN = "ES";
		private static final String SPANISH_UNIVERSITY_PREFIX = "E";
				
		@Override
		public Iterable<KeyCountPair<Integer>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery05.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Integer>> studentCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				Dataset<Student> studentsAtSpain = students.filter(
					students.col("countryOfHostInstitution").equalTo(SPAIN)
					.and(students.col("hostInstitution").startsWith(SPANISH_UNIVERSITY_PREFIX)));
				
				studentsAtSpain.sqlContext().udf().register("STUDENT_AT_SPANISH_UNIVERSITY",
					(UDF1<String, String>)(uni) -> {
						uni = uni.substring(1).trim();
						Matcher uniMatcher = UNI_PATTERN.matcher(uni);
						return uniMatcher.find() ? uniMatcher.group(1) : uni;
					}, DataTypes.StringType );
				
				Dataset<Row> studentsAtSpanishUniversity = studentsAtSpain.withColumn("spanishInstitution",
						functions.callUDF("STUDENT_AT_SPANISH_UNIVERSITY", functions.col("hostInstitution")));
				studentsAtSpanishUniversity = studentsAtSpanishUniversity.groupBy(
						new Column("spanishInstitution")).count().sort("spanishInstitution");
				
				int nrows = (int) studentsAtSpanishUniversity.count();
				studentCount = studentsAtSpanishUniversity.takeAsList(nrows).stream()
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
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery05 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark05-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudentsReceivedBySpanishUniversity(), args);
	}
}
