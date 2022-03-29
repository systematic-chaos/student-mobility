/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 6: Destination country preferred by each country's students.
 * 
 * hadoop - GeneralQuery06.java
 */

package systematicchaos.studentmobility;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJob;
import systematicchaos.studentmobility.hadoop.HadoopJobGraphGenerator;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopJobMonoThreadNode;
import systematicchaos.studentmobility.hadoop.HadoopJobNode;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery06 {
	
	public static class HomeHostCountryMapper extends HadoopMapper<Text, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(1);
		private Text country = new Text();
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, IntWritable> map(String token) {
			KeyValuePair<Text, IntWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				String hostCountry = getStudentHostCountry(student);
				
				if (hostCountry != null) {
					country.set(String.format("%s\t%s", student.getCountryOfHomeInstitution(), hostCountry));
					mapResult = new KeyValuePair<>(country, ONE);
				}
			}
			return mapResult;
		}
		
		private String getStudentHostCountry(Student student) {
			String hostCountry;
			switch (student.getMobilityType()) {
			case STUDY:
				hostCountry = student.getCountryOfHostInstitution();
				break;
			case PLACEMENT:
				hostCountry = student.getCountryOfWorkPlacement();
				break;
			default:
				hostCountry = null;
			}
			return hostCountry != null && !hostCountry.trim().isEmpty() ? hostCountry : null;
		}
	}
	
	public static class HomeHostStudentsCountryMapper extends HadoopMapper<Text, Text> {
		
		private Text homeCountry = new Text();
		private Text hostCountryStudents = new Text();
		
		@Override
		public KeyValuePair<Text, Text> map(String token) {
			KeyValuePair<Text, Text> mapResult = null;
			String[] values = token.split("\t");
			homeCountry.set(values[0]);
			hostCountryStudents.set(String.format("%s\t%-8s", values[1], values[2]));
			mapResult = new KeyValuePair<>(homeCountry, hostCountryStudents);
			return mapResult;
		}
	}
	
	public static class HomeHostCountryReducer extends HadoopReducer<Text, IntWritable> {
		
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
	
	public static class HomeHostStudentsCountryReducer extends HadoopReducer<Text, Text> {
		
		private Text reduceResult = new Text();
		
		@Override
		public Text reduce(Text key, Iterator<Text> values) {
			Text currentElem;
			int currentCount, max = -1;
			while (values.hasNext()) {
				currentElem = values.next();
				currentCount = Integer.parseInt(
						new String(currentElem.getBytes(), StandardCharsets.UTF_8).split("\t")[1].trim());
				if (currentCount > max) {
					reduceResult.set(currentElem);
					max = currentCount;
				}
			}
			return reduceResult;
		}
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery06 ../data/Student_Mobility.csv out/hadoop06-output
	// cat hadoop06-output/part-r-00000 | awk '{ sub(/[ \t]+$/, ""); print }' hadoop06-output/part-r-00000 | tee hadoop06-output/part-r-00000
	public static void main(String[] args) throws Exception {
		HadoopJobGraphGenerator jobGraph = new HadoopJobGraphGenerator() {
			@Override
			public HadoopJobNode createJobGraph(String[] inputPaths, String outputPath) {
				// Individual map-reduce jobs
				HadoopJob<Text, IntWritable> homeHostCountryJob = new HadoopJob<>(HomeHostCountryMapper.class, HomeHostCountryReducer.class, Text.class, IntWritable.class);
				HadoopJob<Text, Text> homeHostStudentsCountryJob = new HadoopJob<>(HomeHostStudentsCountryMapper.class, HomeHostStudentsCountryReducer.class, Text.class, Text.class);
				
				// Predecessor graph node
				HadoopJobNode homeHostCountryNode = new HadoopJobMonoThreadNode("HOME_HOST_COUNTRY", homeHostCountryJob, inputPaths);
				
				// Root graph node
				return new HadoopJobMonoThreadNode(GeneralQuery06.class.getName(), homeHostStudentsCountryJob,
						new HadoopJobNode[] { homeHostCountryNode }, outputPath);
			}
		};
		
		final int exitCode = HadoopJobLauncher.launchHadoopJobGraph(jobGraph, args);
		System.exit(exitCode);
	}
}
