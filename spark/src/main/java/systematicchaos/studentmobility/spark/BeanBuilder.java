/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - spark/BeanBuilder.java
 */

package systematicchaos.studentmobility.spark;

import java.io.InputStream;
import java.io.Serializable;

public interface BeanBuilder<B> extends Serializable {
	B build(InputStream str);
	InputStream dump(B bean);
}
