/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 3: Proportion of exchange students sent/received per country.
 * 
 * hadoop - GeneralQuery03.java
 */

package systematicchaos.studentmobility;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class GeneralQuery03 {

	public static class HomeCountryMapper extends HadoopMapper<Text, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(-1);
		private Text country = new Text();
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, IntWritable> map(String token) {
			KeyValuePair<Text, IntWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				country.set(student.getCountryOfHomeInstitution());
				mapResult = new KeyValuePair<>(country, ONE);
			}
			return mapResult;
		}
	}
	
	public static class HostCountryMapper extends HadoopMapper<Text, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(1);
		private Text country = new Text();
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		private static final Pattern validCountryPattern = Pattern.compile("[A-Z]+");
		
		@Override
		public KeyValuePair<Text, IntWritable> map(String token) {
			KeyValuePair<Text, IntWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				if (validCountryPattern.matcher(student.getCountryOfHostInstitution()).matches()) {
					country.set(student.getCountryOfHostInstitution());
					mapResult = new KeyValuePair<>(country, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class DumbMapper extends HadoopMapper<Text, DoubleWritable> {
		
		private final Text COUNTRY = new Text();
		private final DoubleWritable VALUE = new DoubleWritable();
		
		@Override
		public KeyValuePair<Text, DoubleWritable> map(String countryValue) {
			KeyValuePair<Text, DoubleWritable> mapResult = null;
			String[] values = countryValue.split("\t");
			if (values.length == 2) {
				COUNTRY.set(values[0]);
				VALUE.set(Double.valueOf(values[1]));
				mapResult = new KeyValuePair<>(COUNTRY, VALUE);
			}
			return mapResult;
		}
	}
	
	public static class CountReducer extends HadoopReducer<Text, IntWritable> {
		
		private IntWritable reduceResult = new IntWritable();
		
		@Override
		public IntWritable reduce(Text key, Iterator<IntWritable> values) {
			int count = 0;
			while (values.hasNext()) {
				count += values.next().get();
			}
			reduceResult.set(count);
			return reduceResult;
		}
	}
	
	public static class DivisionReducer extends HadoopReducer<Text, DoubleWritable> {
		
		private DoubleWritable divisionResult = new DoubleWritable();
		
		@Override
		public DoubleWritable reduce(Text key, Iterator<DoubleWritable> values) {
			ArrayList<Double> valuesList = new ArrayList<>(2);
			values.forEachRemaining(d -> valuesList.add(d.get()));
			if (valuesList.size() == 2) {
				divisionResult.set(-(valuesList.get(0) < 0 ?
						valuesList.get(0) / valuesList.get(1) :
							valuesList.get(1) / valuesList.get(0)));
			} else {
				divisionResult.set(valuesList.get(0));
			}
			return divisionResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery03 ../data/Student_Mobility.csv out/hadoop03-output
	public static void main(String[] args) throws Exception {
		HadoopJobGraphGenerator jobGraph = new HadoopJobGraphGenerator() {
			@Override
			public HadoopJobNode createJobGraph(String[] inputPaths, String outputPath) {
				// Individual map-reduce jobs
				HadoopJob<Text, IntWritable> homeCountryCountJob = new HadoopJob<>(HomeCountryMapper.class, CountReducer.class, Text.class, IntWritable.class);
				HadoopJob<Text, IntWritable> hostCountryCountJob = new HadoopJob<>(HostCountryMapper.class, CountReducer.class, Text.class, IntWritable.class);
				HadoopJob<Text, DoubleWritable> divisionJob = new HadoopJob<>(DumbMapper.class, DivisionReducer.class, Text.class, DoubleWritable.class);
				
				// Predecessor graph nodes
				HadoopJobNode homeCountryNode = new HadoopJobMultiThreadNode("HOMEINSTITUTION", homeCountryCountJob, inputPaths);
				HadoopJobNode hostCountryNode = new HadoopJobMultiThreadNode("HOSTINSTITUTION", hostCountryCountJob, inputPaths);
				
				// Root graph node
				return new HadoopJobMultiThreadNode(GeneralQuery03.class.getName(), divisionJob,
						new HadoopJobNode[] { homeCountryNode, hostCountryNode }, outputPath);
			}
		};
		
		final int exitCode = HadoopJobLauncher.launchHadoopJobGraph(jobGraph, args);
		System.exit(exitCode);
	}
}
