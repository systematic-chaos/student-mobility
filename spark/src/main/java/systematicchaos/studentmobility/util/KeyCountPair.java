/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - util/KeyCountPair.java
 */

package systematicchaos.studentmobility.util;

import java.util.AbstractMap;

public class KeyCountPair<N extends Number> extends AbstractMap.SimpleImmutableEntry<String, N> {
	
	private static final long serialVersionUID = -5051409633585634092L;

	public KeyCountPair(String key, N count) {
		super(key, count);
	}
	
	@Override
	public String toString() {
		return this.getKey() + '\t' + this.getValue();
	}
}
