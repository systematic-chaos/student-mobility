/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * General query 15: Total exchange students having been taught per teaching language.
 * 
 * hadoop - GeneralQuery15.java
 */

package systematicchaos.studentmobility;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.hadoop.HadoopJobLauncher;
import systematicchaos.studentmobility.hadoop.HadoopMapper;
import systematicchaos.studentmobility.hadoop.HadoopReducer;
import systematicchaos.studentmobility.util.KeyValuePair;

public class GeneralQuery15 {
	
	public static class TeachingLanguageMapper extends HadoopMapper<Text, LongWritable> {
		
		private static final LongWritable ONE = new LongWritable(1l);
		private Text teachingLanguage = new Text();
		
		private static final String HEADER = "HOMEINSTITUTION";
		
		@Override
		public KeyValuePair<Text, LongWritable> map(String token) {
			KeyValuePair<Text, LongWritable> mapResult = null;
			if (!token.startsWith(HEADER)) {
				Student student = Student.fromString(token);
				String languageTaught = student.getLanguageTaught();
				if (languageTaught != null && languageTaught.length() > 0) {
					teachingLanguage.set(languageTaught.toUpperCase());
					mapResult = new KeyValuePair<>(teachingLanguage, ONE);
				}
			}
			return mapResult;
		}
	}
	
	public static class TeachingLanguageReducer extends HadoopReducer<Text, LongWritable> {
		
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
	
	// hadoop jar target/hadoop-1.0.0.jar systematicchaos.studentmobility.GeneralQuery15 ../data/Student_Mobility.csv out/hadoop15-output
	public static void main(String[] args) throws Exception {
		final int exitCode = HadoopJobLauncher.launchHadoopJob(
				GeneralQuery15.class.getName(),
				TeachingLanguageMapper.class, TeachingLanguageReducer.class,
				Text.class, LongWritable.class, args);
		System.exit(exitCode);
	}
}
