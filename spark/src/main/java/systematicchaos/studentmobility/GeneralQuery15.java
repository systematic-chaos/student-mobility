/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 15: Total exchange students having been taught per teaching language.
 * 
 * spark - GeneralQuery15.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;

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

public class GeneralQuery15 {
	
	public static class TeachingLanguage implements SparkTask<Long> {
		
		@Override
		public Iterable<KeyCountPair<Long>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery15.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Long>> languageCount = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				students.sqlContext().udf().register("TEACHING_LANGUAGE",
						(UDF1<String, String>)(languageTaught) -> {
							return languageTaught != null && languageTaught.length() > 0 ?
									languageTaught.toUpperCase() : null;
						}, DataTypes.StringType);
				Column langCol = new Column("teachingLanguage");
				Dataset<Row> languageTaught = students.withColumn("teachingLanguage",
						callUDF("TEACHING_LANGUAGE",
								new Column("languageTaught")))
						.filter(langCol.isNotNull())
						.groupBy(langCol).count().sort(langCol);
				
				int nrows = (int) languageTaught.count();
				languageCount = languageTaught.takeAsList(nrows).stream()
						.map((lt) -> new KeyCountPair<>(lt.getString(0), lt.getLong(1)))
						.collect(Collectors.toList());
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return languageCount;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery15 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark15-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new TeachingLanguage(), args);
	}
}
