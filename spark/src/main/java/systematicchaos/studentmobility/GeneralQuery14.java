/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 14: Proportion, of those students that initially stay abroad just for the
 * fall semester, that extend their exchange period to the next spring semester.
 * 
 * spark - GeneralQuery14.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;
import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.SparkJob;
import systematicchaos.studentmobility.spark.SparkJobLauncher;
import systematicchaos.studentmobility.spark.SparkTask;
import systematicchaos.studentmobility.util.KeyCountPair;
import systematicchaos.studentmobility.util.StudentBuilder;

public class GeneralQuery14 {
	
	public static class PlacementExtensionProportion implements SparkTask<Double> {
		
		@Override
		public Iterable<KeyCountPair<Double>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery14.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Double>> extensionProportion = new ArrayList<>(2);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				double studentCount = students.count();
				
				students.sqlContext().udf().register("PLACEMENT_EXTENSION",
						(UDF5<String, String, String, Float, Float, String>)(mobilityType, studyStartDate, placementStartDate, studyLength, placementLength) -> {
							Student student = new Student();
							student.setMobilityType(Student.MobilityType.valueOf(mobilityType));
							student.setStudyStartDate(studyStartDate);
							student.setPlacementStartDate(placementStartDate);
							student.setLengthStudyPeriod(studyLength);
							student.setLengthWorkPlacement(placementLength);
							boolean placementExtension = getPlacementExtension(student);
							return String.valueOf(placementExtension ? 'Y' : 'N');
						}, DataTypes.StringType);
				
				Column extensionCol = new Column("extension");
				Dataset<Row> placementExtensionProportion = students.withColumn("extension", callUDF("PLACEMENT_EXTENSION",
						new Column("mobilityType"), new Column("studyStartDate"), new Column("placementStartDate"), new Column("lengthStudyPeriod"), new Column("lengthWorkPlacement")))
						.groupBy(extensionCol).count().orderBy(extensionCol);
				
				placementExtensionProportion.takeAsList(2).forEach(
						extension -> extensionProportion.add(new KeyCountPair<>(
								extension.getString(0),
								roundHalfDown(extension.getLong(1) / studentCount))));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return extensionProportion;
		}
		
		private static boolean getPlacementExtension(Student student) {
			LocalDate startDate = getStudyPlacementStartDate(student);
			Float length = getStudyPlacementLength(student);
			return length != null && length >= 7f
					&& startDate != null && startDate.getMonthValue() > 6;
		}
		
		private static LocalDate getStudyPlacementStartDate(Student student) {
			String startDate;
			switch (student.getMobilityType()) {
			case STUDY:
				startDate = student.getStudyStartDate();
				break;
			case PLACEMENT:
				startDate = student.getPlacementStartDate();
				break;
			default:
				startDate = null;
			}
			return startDate != null ? Student.dateFromString(startDate) : null;
		}
		
		private static Float getStudyPlacementLength(Student student) {
			Float length;
			switch (student.getMobilityType()) {
			case STUDY:
				length = student.getLengthStudyPeriod();
				break;
			case PLACEMENT:
				length = student.getLengthWorkPlacement();
				break;
			default:
				length = null;
			}
			return length;
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery14 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark14-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new PlacementExtensionProportion(), args);
	}
}
