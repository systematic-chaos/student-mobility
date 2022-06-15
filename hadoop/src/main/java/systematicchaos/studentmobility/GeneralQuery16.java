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
 * hadoop - GeneralQuery16.java
 */

package systematicchaos.studentmobility;

import static systematicchaos.studentmobility.util.Functions.roundHalfDown;

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

public class GeneralQuery16 {
	
	private static final String YES_VALUE = String.valueOf('Y');
	private static final String NO_VALUE = String.valueOf('N');
	
	public static class ForeignLanguageTrainingMapper extends HadoopMapper<Text, LongWritable> {
		
		private static final LongWritable ONE = new LongWritable(1l);
		private Text language = new Text();
		
		private static final String HEADER = "HOMEINSTITUTION";
		private static final String ENGLISH = "EN";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				
				String languageTaught = student.getLanguageTaught();
				languageTaught = languageTaught != null && languageTaught.length() > 0 ?
						languageTaught.toUpperCase() : null;
				if (ENGLISH.equals(languageTaught)) {
					// Key is the language and whether the student was taught that language
					language.set(String.format("%s\t%s", languageTaught,
							Boolean.TRUE.equals(student.getTaughtHostLang()) ? YES_VALUE : NO_VALUE));
					mapResult = new KeyValuePair<>(language, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class ForeignLanguageTrainingReducer extends HadoopReducer<Text, LongWritable> {
		
		private LongWritable reduceResult = new LongWritable();
		
		@Override
		public LongWritable reduce(Text key, Iterator<LongWritable> values) {
			long count = 0l;
			while (values.hasNext()) {
				count += values.next().get();
			}
			reduceResult.set(count);
			return reduceResult;
		}
	}
	
	public static class LanguageTrainingCountMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private Text language = new Text();
		private DoubleWritable trainingCount = new DoubleWritable();
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String token) {
			String[] row = token.split("\t");
			int mapCount = Integer.valueOf(row[2]);
			language.set(row[0]);
			// Returning a positive value denotes it as the YES_VALUE aggregate count;
			// while returning a negative value denotes it as the NO_VALUE aggregate count.
			trainingCount.set((double)(YES_VALUE.equals(row[1]) ? mapCount : -mapCount));
			return new KeyValuePair<>(language, trainingCount);
		}
	}
	
	public static class LanguageTrainingProportionReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private DoubleWritable reduceResult = new DoubleWritable();
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			Double s = null, n = null;
			while (values.hasNext()) {
				double val = values.next().get();
				if (val >= 0) {
					s = val;
				} else {
					n = val;
				}
			}
			
			double reduction;
			// If both values (a positive and a negative, assigned to s and n, respectively) 
			// are provided, having been included in the input iterator.
			if (s != null && n != null) {
				// The language training proportion is actually calculated as "s / (s + n)"
				// Here, n is subtracted (instead of being added to s) since it is a negative value,
				// being set as such in the preceding mapper for students not having been taught the key language.
				reduction = s / (s - n);
			} else {
				// If just a value was provided, return that value
				// (n will still be returned as a negative value).
				reduction = s == null ? n : s;
			}
			reduceResult.set(roundHalfDown(reduction));
			return reduceResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery16 ../data/Student_Mobility.csv out/hadoop16-output
	public static void main(String[] args) throws Exception {
		HadoopJobGraphGenerator jobGraph = new HadoopJobGraphGenerator() {
			@Override
			public HadoopJobNode createJobGraph(String[] inputPaths, String outputPath) {
				// Individual map-reduce jobs
				HadoopJob<Text, LongWritable> foreignLanguageJob = new HadoopJob<>(
						ForeignLanguageTrainingMapper.class, ForeignLanguageTrainingReducer.class,
						Text.class, LongWritable.class);
				HadoopJob<Text, DoubleWritable> languageTrainingJob = new HadoopJob<>(
						LanguageTrainingCountMapper.class, LanguageTrainingProportionReducer.class,
						Text.class, DoubleWritable.class);
				
				// Predecessor graph node
				HadoopJobNode foreignLanguageNode = new HadoopJobMultiThreadNode(
						"FOREIGN_LANGUAGE", foreignLanguageJob, inputPaths);
				
				// Root graph node
				return new HadoopJobMultiThreadNode(
						GeneralQuery16.class.getName(), languageTrainingJob,
						new HadoopJobNode[] { foreignLanguageNode }, outputPath);
			}
		};
		
		final int exitCode = HadoopJobLauncher.launchHadoopJobGraph(jobGraph, args);
		System.exit(exitCode);
	}
}
