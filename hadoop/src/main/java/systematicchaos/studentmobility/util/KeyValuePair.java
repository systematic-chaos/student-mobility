/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - util/KeyValuePair.java
 */

package systematicchaos.studentmobility.util;

import java.util.AbstractMap;

public class KeyValuePair<K, V> extends AbstractMap.SimpleEntry<K, V> {
	
	private static final long serialVersionUID = -6205555241909640832L;

	public KeyValuePair(K key, V value) {
		super(key, value);
	}
	
	public V setValue(V newValue) {
		V oldValue = this.getValue();
		super.setValue(newValue);
		return oldValue;
	}
}