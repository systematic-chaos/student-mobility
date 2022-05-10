/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 16: Teaching languages used and proportion of students that, having been taught
 * in that language, received specific training in that language's utilization.
 * This query has been restricted to the English language, for the sake of simplicity
 * in the interpretation of results and data consistency.
 * 
 * spark - GeneralQuery16.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.util.ArrayList;
import java.util.List;

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

public class GeneralQuery16 {
	
	private static final String ENGLISH = "EN";
	
	public static class ForeignLanguageTrainingProportion implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery16.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> languageTeachingProportion = null;
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				students.sqlContext().udf().register("LANGUAGE_TAUGHT",
						(UDF1<String, String>)(languageTaught) -> {
							return languageTaught != null && languageTaught.trim().length() > 0 ? languageTaught.toUpperCase() : null;
						}, DataTypes.StringType);
				students.sqlContext().udf().register("TAUGHT_HOST_LANGUAGE",
						(UDF1<Boolean, Boolean>)(taughtHostLanguage) -> {
							return Boolean.TRUE.equals(taughtHostLanguage);
						}, DataTypes.BooleanType);
				
				Column langTaughtCol = new Column("langTaught");
				Column taughtHostLangCol = new Column("taughtHostLanguage");
				Dataset<Row> taughtLangStudents = students.withColumn("langTaught", callUDF("LANGUAGE_TAUGHT", new Column("languageTaught")))
						.withColumn("taughtHostLanguage", callUDF("TAUGHT_HOST_LANGUAGE", new Column("taughtHostLang")))
						.filter(langTaughtCol.equalTo(ENGLISH))
						.groupBy(langTaughtCol, taughtHostLangCol).count().sort(langTaughtCol, taughtHostLangCol);
				
				languageTeachingProportion = computeLanguageTeachingProportion(taughtLangStudents.takeAsList((int) taughtLangStudents.count()));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return languageTeachingProportion;
		}
		
		private List<KeyCountPair<Double>> computeLanguageTeachingProportion(List<Row> taughtLangStudents) {
			int nrows = taughtLangStudents.size();
			List<KeyCountPair<Double>> languageTeachingProportion = new ArrayList<>(nrows);
			
			Row row;
			String language;
			double taughtYes, taughtNo;
			for (int n = 0; n < nrows; n++) {
				row = taughtLangStudents.get(n);
				language = row.getString(0);
				if (row.getBoolean(1)) {	// Only true group value is available
					taughtYes = (double) row.getLong(2);
					taughtNo = 0.;
				} else {
					taughtNo = (double) row.getLong(2);
					taughtYes = nrows > n + 1 && language.equals(taughtLangStudents.get(n + 1).getString(0)) ?
							(double) taughtLangStudents.get(++n).getLong(2)	// Both group values are available
							: 0.;	// Only false group value is available
				}
				languageTeachingProportion.add(new KeyCountPair<>(language, roundHalfDown(taughtYes / (taughtYes + taughtNo))));
			}
			return languageTeachingProportion;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery16 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark16-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new ForeignLanguageTrainingProportion(), args);
	}
}
