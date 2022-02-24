/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - util/Functions.java
 */

package systematicchaos.studentmobility.util;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class Functions {

	public static double roundHalfDown(double n) {
		return Functions.roundHalfDown(n, 3);
	}
	
	public static double roundHalfDown(double n, int decimals) {
		DecimalFormat df = new DecimalFormat(String.format("#.%0" + decimals + "d", 0).replace('0', '#'),
				DecimalFormatSymbols.getInstance(Locale.US));
		df.setRoundingMode(RoundingMode.HALF_DOWN);
		return Double.valueOf(df.format(n));
	}
}
