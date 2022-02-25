/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - spark/SparkJob.java
 */

package systematicchaos.studentmobility.spark;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJob<Bean> {
	
	private SparkSession spark;
	private Class<Bean> beanClass;
	private BeanBuilder<Bean> beanBuilder;
	
	private static final String CSV_EXTENSION = "csv";
	public static final String CSV_SPLIT = ";";
	
	private static final String OUTPUT_FILE = "part-r-00000";
	private static final String SUCCESS_FILE = "_SUCCESS";
	private static final String FAILURE_FILE = "_FAILURE";
	
	public SparkJob(String name, Class<Bean> beanClass, BeanBuilder<Bean> beanBuilder) {
		this.spark = this.buildSparkSession(name);
		this.beanClass = beanClass;
		this.beanBuilder = beanBuilder;
	}
	
	public void launchSparkJob(String[] input) {
		try {
			Dataset<Bean> dataset = this.readDataset(input).cache();
			SparkJob.printOutput(dataset.takeAsList((int) dataset.count()));
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
	
	public void launchSparkJob(String[] input, String output) {
		try {
			Dataset<Bean> dataset = this.readDataset(input).cache();
			List<Bean> outputData = dataset.takeAsList((int) dataset.count());
			SparkJob.writeOutput(output, outputData);
			SparkJob.printOutput(outputData);
		} catch (Exception e) {
			SparkJob.writeFailure(output);
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
	
	public Dataset<Bean> readDataset(String[] inputFiles) {
		DataFrameReader csvReader = this.createCsvReader(this.spark);
		
		Dataset<Bean> accumulatedDataset = null;
		Row2BeanMapper<Bean> mapper = new Row2BeanMapper<>(this.beanBuilder);
		for (String path : inputFiles) {
			Dataset<Row> csv = csvReader.load("file://" + path);
			Dataset<Bean> beans = csv.map(mapper, Encoders.bean(beanClass));
			accumulatedDataset = accumulatedDataset == null ? beans : accumulatedDataset.union(beans);
		}
		
		return accumulatedDataset;
	}
	
	public static void printOutput(Iterable<?> outputData) {
		outputData.forEach((data) -> {
			System.out.println(data.toString());
		});
	}
	
	public static void writeOutput(String outputPath, Iterable<?> outputData) throws IOException {
		File outputDir = new File(outputPath);
		
		// Create new empty output directory, clearing its contents in case it already exists
		if (outputDir.exists() && outputDir.isFile()) {
			outputDir.delete();
		}
		if (!outputDir.mkdir()) {
			new File(outputDir, OUTPUT_FILE).delete();
			new File(outputDir, SUCCESS_FILE).delete();
			new File(outputDir, FAILURE_FILE).delete();
		}
		
		File outputFile = new File(outputDir, OUTPUT_FILE);
		PrintWriter writer = new PrintWriter(Files.newOutputStream(outputFile.toPath(),
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING), true);
		outputData.forEach((data) -> {
			writer.println(data.toString());
		});
		writer.close();
		
		new File(outputDir, SUCCESS_FILE).createNewFile();
	}
	
	private SparkSession buildSparkSession(String appName) {
		return SparkSession.builder()
				.appName(appName)
				.master("local[*]")
				.getOrCreate();
	}
	
	private DataFrameReader createCsvReader(SparkSession spark) {
		return spark.read()
				.format(CSV_EXTENSION)
				.option("sep", CSV_SPLIT)
				.option("header", true)
				.option("inferSchema", false);
	}
	
	public void close() {
		this.spark.stop();
	}
	
	public static void writeFailure(String outputPath) {
		try {
			File outputFile = new File(outputPath, FAILURE_FILE);
			outputFile.createNewFile();
		} catch (IOException ioe) {
			System.err.println("IOException creating failure file: " + ioe.getMessage());
		}
	}
	
	private static class Row2BeanMapper<B> implements MapFunction<Row, B> {
		private BeanBuilder<B> beanBuilder;
		private static final long serialVersionUID = 3918430196823865704L;
		
		public Row2BeanMapper(BeanBuilder<B> beanBuilder) {
			this.beanBuilder = beanBuilder;
		}
		
		public B call(Row rowValue) {
			String str = rowValue.mkString(CSV_SPLIT).replace("null", "");
			InputStream stream = new ByteArrayInputStream(str.getBytes());
			return this.beanBuilder.build(stream);
		}
	}
}
