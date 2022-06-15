/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 13: Total of exchange students that stay abroad for the fall semester,
 * the spring semester, and the whole academic year, respectively.
 * 
 * spark - GeneralQuery13.java
 */

package systematicchaos.studentmobility;

import static org.apache.spark.sql.functions.callUDF;

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

public class GeneralQuery13 {
	
	public static class StudyPlacementLength implements SparkTask<Long> {
		
		@Override
		public Iterable<KeyCountPair<Long>> computeSparkTask(String[] inputFiles) {
			SparkJob<Student> job = new SparkJob<>(GeneralQuery13.class.getName(),
					Student.class, new StudentBuilder());
			List<KeyCountPair<Long>> studentCount = new ArrayList<>(3);
			
			try {
				Dataset<Student> students = job.readDataset(inputFiles);
				
				students.sqlContext().udf().register("PERIOD_LENGTH",
						(UDF5<String, String, String, Float, Float, String>)(mobilityType, studyStartDate, placementStartDate, studyLength, placementLength) -> {
							Student student = new Student();
							student.setMobilityType(Student.MobilityType.valueOf(mobilityType));
							student.setStudyStartDate(studyStartDate);
							student.setPlacementStartDate(placementStartDate);
							student.setLengthStudyPeriod(studyLength);
							student.setLengthWorkPlacement(placementLength);
							return StudyPlacementLength.getPeriodLength(student);
						}, DataTypes.StringType);
				
				Column periodCol = new Column("PERIOD");
				Dataset<Row> studyPlacementLength = students.withColumn("period", callUDF("PERIOD_LENGTH",
						new Column("mobilityType"), new Column("studyStartDate"), new Column("placementStartDate"), new Column("lengthStudyPeriod"), new Column("lengthWorkPlacement")))
						.filter(periodCol.isNotNull())
						.groupBy(periodCol).count().orderBy(periodCol);
				
				studyPlacementLength.takeAsList(3).forEach(
						period -> studentCount.add(new KeyCountPair<>(
								period.getString(0), period.getLong(1))));
			} catch (Exception e) {
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
			} finally {
				job.close();
			}
			
			return studentCount;
		}
		
		private static String getPeriodLength(Student student) {
			LocalDate startDate = getStudyPlacementStartDate(student);
			Float length = getStudyPlacementLength(student);
			PeriodLength periodLength = null;
			if (length != null && length >= 7f) {
				periodLength = PeriodLength.FULL;
			} else if (startDate != null) {
				if (startDate.getMonthValue() > 6) {
					periodLength = length != null && length > 0f ? PeriodLength.FIRST : PeriodLength.FULL;
				} else {
					periodLength = PeriodLength.SECOND;
				}
			}
			return periodLength != null ? String.valueOf(periodLength.getValue()) : null;
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
		
		private static enum PeriodLength {
			FIRST('1'),
			SECOND('2'),
			FULL('F');
			
			private char value;
			
			private PeriodLength(char value) {
				this.value = value;
			}
			
			public char getValue() {
				return this.value;
			}
			
			@Override
			public String toString() {
				return this.name();
			}
		}
	}
	
	// spark-submit --class systematicchaos.studentmobility.GeneralQuery13 --master local[*] target/spark-1.0.0.jar ../data/Student_Mobility.csv [out/spark13-output]
	public static void main(String[] args) throws Exception {
		SparkJobLauncher.launchSparkJob(new StudyPlacementLength(), args);
	}
}
