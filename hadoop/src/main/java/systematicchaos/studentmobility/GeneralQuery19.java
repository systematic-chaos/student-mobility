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
 * hadoop - GeneralQuery19.java
 */

package systematicchaos.studentmobility;

import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJob;
import systematicchaos.studentmobility.hadoop.HadoopJobGraphGenerator;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopJobMultiThreadNode;
import systematicchaos.studentmobility.hadoop.HadoopJobNode;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery19 {
	
	private static long studentTotalCount;
	
	public static class MobilityLevelMapper extends HadoopMapper<Text, LongWritable> {
		
		private Text studyWorkLevel = new Text();
		private static final LongWritable ONE = new LongWritable(1l);
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				Student.MobilityType mobilityType = student.getMobilityType();
				Student.StudyLevel studyLevel = student.getStudyLevel();
				
				if (mobilityType != null && studyLevel != null) {
					studyWorkLevel.set(String.format("%c\t%c",
							mobilityType.getValue(), studyLevel.getValue()));
					mapResult = new KeyValuePair<>(studyWorkLevel, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class MobilityLevelReducer extends HadoopReducer<Text, LongWritable> {
		
		private LongWritable mobilityLevelCount = new LongWritable();
		
		@Override
		public LongWritable reduce(Text key, Iterator<LongWritable> values) {
			long count = 0l;
			while (values.hasNext()) {
				count += values.next().get();
			}
			mobilityLevelCount.set(count);
			return mobilityLevelCount;
		}
	}
	
	public static class TotalCountMapper extends HadoopMapper<Text, LongWritable> {
		
		private static final Text TOTAL_KEY = new Text(GeneralQuery19.class.getName());
		private static final LongWritable ONE = new LongWritable(1l);
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				Student.MobilityType mobilityType = student.getMobilityType();
				Student.StudyLevel studyLevel = student.getStudyLevel();
				
				if (mobilityType != null && studyLevel != null) {
					// Just for accounting the total number of students computed
					mapResult = new KeyValuePair<>(TOTAL_KEY, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class TotalCountReducer extends HadoopReducer<Text, LongWritable> {
		
		private LongWritable totalCount = new LongWritable();
		
		@Override
		public LongWritable reduce(Text key, Iterator<LongWritable> values) {
			long count = 0l;
			while (values.hasNext()) {
				count += values.next().get();
			}
			
			// Store last aggregate count in upper-level static variable
			studentTotalCount = count;
			
			totalCount.set(count);
			return totalCount;
		}
	}
	
	public static class MobilityTypeStudyLevelMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private Text mobilityTypeStudyLevel = new Text();
		private DoubleWritable count = new DoubleWritable();
		
		private static final String TOTAL_KEY = GeneralQuery19.class.getName();
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String token) {
			KeyValuePair<Text, DoubleWritable> mapResult = null;
			
			// Ignore the aggregate total count row, it is received here for setting precedence
			// and getting step 2 results before executing step3, but that value is actually
			// stored in a static variable by the step 2 reducer.
			if (!token.startsWith(TOTAL_KEY)) {
				String[] row = token.split("\t");
				// Key is composed of mobility type and study level
				mobilityTypeStudyLevel.set(String.format("%s\t%s", row[0], row[1]));
				count.set(Double.valueOf(row[2]));
				mapResult = new KeyValuePair<>(mobilityTypeStudyLevel, count);
			}
			return mapResult;
		}
	}
	
	public static class MobilityTypeStudyLevelReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private DoubleWritable proportion = new DoubleWritable();
		private static HashMap<String, Double> reduceResultTable = new HashMap<>();
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			String keyValue = new String(key.getBytes(), StandardCharsets.UTF_8);
			
			// If entry is empty, calculate and assign proportion
			if (reduceResultTable.get(keyValue) == null) {
				reduceResultTable.put(keyValue,
						roundHalfDown(values.next().get() / studentTotalCount));
			}
			
			// Anyway, return value stored in the hash-map for that entry
			proportion.set(reduceResultTable.get(keyValue));
			return proportion;
		}
	}

	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery19 ../data/Student_Mobility.csv out/hadoop19-output
	public static void main(String[] args) throws Exception {
		HadoopJobGraphGenerator jobGraph = new HadoopJobGraphGenerator() {
			@Override
			public HadoopJobNode createJobGraph(String[] inputPaths, String outputPath) {
				// Individual map-reduce jobs
				HadoopJob<Text, LongWritable> mobilityLevelJob = new HadoopJob<>(
						MobilityLevelMapper.class, MobilityLevelReducer.class,
						Text.class, LongWritable.class);
				HadoopJob<Text, LongWritable> totalCountJob = new HadoopJob<>(
						TotalCountMapper.class, TotalCountReducer.class,
						Text.class, LongWritable.class);
				HadoopJob<Text, DoubleWritable> mobilityTypeStudyLevelJob = new HadoopJob<>(
						MobilityTypeStudyLevelMapper.class, MobilityTypeStudyLevelReducer.class,
						Text.class, DoubleWritable.class);
				
				// Predecessor graph node
				HadoopJobNode mobilityLevelNode = new HadoopJobMultiThreadNode(
						"MOBILITY_LEVEL", mobilityLevelJob, inputPaths);
				HadoopJobNode totalCountNode = new HadoopJobMultiThreadNode(
						"TOTAL_COUNT", totalCountJob, inputPaths);
				
				// Root graph node
				return new HadoopJobMultiThreadNode(GeneralQuery19.class.getName(), mobilityTypeStudyLevelJob,
						new HadoopJobNode[] { mobilityLevelNode, totalCountNode }, outputPath);
			}
		};
		
		final int exitCode = HadoopJobLauncher.launchHadoopJobGraph(jobGraph, args);
		System.exit(exitCode);
	}
}
