/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * Specific query 1: How many students, from the same home country as the 22-aged individual,
 * whose home university code is "CIUDA-R" and whose host university code is "S VASTERA",
 * took their Erasmus exchange in the same host university.
 * 
 * hadoop - SpecificQuery1.java
 */

package systematicchaos.studentmobility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJob;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.Functions;
import systematicchaos.studentmobility.util.KeyValuePair;

public class SpecificQuery1 {
	
	private static Student me;
	
	public static class SubjectIdentificationMapper extends HadoopMapper<Text, Text> {
		
		private Text homeCountry = new Text();
		private Text hostCountry = new Text();
		
		private static final String HOME_UNIVERSITY = "CIUDA";
		private static final String HOST_UNIVERSITY = "VASTERA";
		private static final int AGE = 22;
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, Text> map(String token) {
			KeyValuePair<Text, Text> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				if (student.getHomeInstitution().length() > 3
						&& student.getHomeInstitution().substring(3).startsWith(HOME_UNIVERSITY)
						&& student.getHostInstitution().length() > 3
						&& student.getHostInstitution().substring(3).startsWith(HOST_UNIVERSITY)
						&& Integer.valueOf(AGE).equals(student.getAge())) {
					homeCountry.set(student.getCountryOfHomeInstitution());
					hostCountry.set(student.getCountryOfHostInstitution());
					mapResult = new KeyValuePair<>(homeCountry, hostCountry);
				}
			}
			return mapResult;
		}
	}
	
	public static class CountryCompanionsMapper extends HadoopMapper<Text, LongWritable> {
		
		private static final Text COUNTRY_COMPANIONS = new Text("COUNTRY_COMPANIONS");
		private static final LongWritable ONE = new LongWritable(1l);
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				if (SpecificQuery1.me.getCountryOfHomeInstitution().equals(student.getCountryOfHomeInstitution())
						&& SpecificQuery1.me.getCountryOfHostInstitution().equals(student.getCountryOfHostInstitution())) {
					mapResult = new KeyValuePair<>(COUNTRY_COMPANIONS, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class SubjectIdentificationReducer extends HadoopReducer<Text, Text> {
		
		private Text reduceResult = new Text();
		
		@Override
		public Text reduce(Text key, Iterator<Text> values) {
			if (values.hasNext()) {
				reduceResult.set(values.next());
			}
			return reduceResult;
		}
	}
	
	public static class CountryCompanionsReducer extends HadoopReducer<Text, LongWritable> {
		
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
	
	private static Student readJobOutput(String path) throws IOException {
		Student subject = new Student();
		try (BufferedReader reader = new BufferedReader(
				new FileReader(Paths.get(path, "part-r-00000").toFile()))) {
			String[] studentFields = reader.readLine().split("\t");
			subject.setCountryOfHomeInstitution(studentFields[0]);
			subject.setCountryOfHostInstitution(studentFields[1]);
		}
		return subject;
	}
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.SpecificQuery1 ../data/Student_Mobility.csv out/hadoop-1-output
	public static void main(String[] args) throws Exception {
		// Individual map-reduce jobs
		HadoopJob<Text, Text> subjectIdentificationJob = new HadoopJob<>(
				SubjectIdentificationMapper.class, SubjectIdentificationReducer.class,
				Text.class, Text.class);
		HadoopJob<Text, LongWritable> countryCompanionsJob = new HadoopJob<>(
				CountryCompanionsMapper.class, CountryCompanionsReducer.class,
				Text.class, LongWritable.class);
		
		// Launch subject identification job, setting its output path properly
		String subjectIdentificationJobKey = "CIUDA-R VASTERA 22";
		final String[] inputArgs = Arrays.copyOf(args, args.length);
		inputArgs[inputArgs.length - 1] = subjectIdentificationJobKey;
		int jobCompletionCode = HadoopJobLauncher.launchHadoopJob(
				subjectIdentificationJobKey, subjectIdentificationJob, inputArgs);
		
		// If first job completes successfully, launch match and count job for country companions
		if (jobCompletionCode == 0) {
			// Read and fill data for subject student identified
			SpecificQuery1.me = SpecificQuery1.readJobOutput(subjectIdentificationJobKey);
			Functions.removeOutputDirectory(subjectIdentificationJobKey);
			
			jobCompletionCode = HadoopJobLauncher.launchHadoopJob(
				SpecificQuery1.class.getName(), countryCompanionsJob, args);
		}
		
		System.exit(jobCompletionCode);
	}
}
