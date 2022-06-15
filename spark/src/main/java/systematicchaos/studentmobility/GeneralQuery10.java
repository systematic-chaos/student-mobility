/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 10: Gender proportion of European exchange students.
 * 
 * spark - GeneralQuery10.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.col;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery10 {
	
	public static class GenderProportion implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery10.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> studentGenderProportion = new ArrayList<>(2);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				final double studentCount = (double) students.count();
				Dataset<Row> studentsGender = students.groupBy(col("gender")).count();
				
				studentsGender.orderBy(asc("gender")).takeAsList(2).forEach(
						s -> studentGenderProportion.add(new KeyCountPair<>(
								String.valueOf(s.getString(0).charAt(0)),
								roundHalfDown(s.getLong(1) / studentCount))));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return studentGenderProportion;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery10 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark10-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new GenderProportion(), args);
	}
}
