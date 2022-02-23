/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - util/Functions.java
 */

package systematicchaos.studentmobility.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class Functions {
	
	static final String OUTPUT_FILE = "part-r-00000";
	
	public static void printOutput(String outputPath) {
		try (BufferedReader br = new BufferedReader(new FileReader(Paths.get(outputPath, OUTPUT_FILE).toFile()))) {
			String line;
			while ((line = br.readLine()) != null) {
				System.out.println(line.trim());
			}
		} catch (IOException ioe) {
			System.err.println("Error printing data to standard output stream: " + ioe.getMessage());
		}
	}
	
	public static void removeOutputDirectory(String outputPath) {
		File outputDir = new File(outputPath);
		if (outputDir.exists()) {
			if (outputDir.isDirectory()) {
				for (File f : outputDir.listFiles()) {
					if (f.isDirectory()) {
						removeOutputDirectory(f.getAbsolutePath());
					} else {
						f.delete();
					}
				}
			}
			outputDir.delete();
		}
	}
	
	public static void trimTextFile(String filePath) {
		StringBuilder str = new StringBuilder();
		File file = Paths.get(filePath, OUTPUT_FILE).toFile();
		try {
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = reader.readLine()) != null) {
					line = line.trim();
					if (line.length() > 0) {
						str.append(line + '\n');
					}
				}
			}
			
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
				writer.write(str.toString(), 0, str.length());
			}
		} catch (IOException ioe) {
			System.err.println("IOException trimming output file: " + ioe.getMessage());
		}
	}
	
	public static double roundHalfDown(double n) {
		return Functions.roundHalfDown(n, 3);
	}
	
	public static double roundHalfDown(double n, int decimals) {
		DecimalFormat df = new DecimalFormat(String.format("#.%0" + decimals + "d",  0).replace('0', '#'),
				DecimalFormatSymbols.getInstance(Locale.US));
		df.setRoundingMode(RoundingMode.HALF_DOWN);
		return Double.valueOf(df.format(n));
	}
}
