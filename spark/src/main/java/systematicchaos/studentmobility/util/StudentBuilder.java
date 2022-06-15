/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - util/StudentBuilder.java
 */

package systematicchaos.studentmobility.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import systematicchaos.studentmobility.domain.Student;
import systematicchaos.studentmobility.spark.BeanBuilder;

public class StudentBuilder implements BeanBuilder<Student> {
	
	private static final long serialVersionUID = -8214418819135163277L;

	public Student build(InputStream str) {
		Student student = null;
		try (Scanner scanner = new Scanner(str, StandardCharsets.UTF_8.name())) {
			student = Student.fromString(scanner.useDelimiter("\\A").next());
		}
		return student;
	}
	
	public InputStream dump(Student s) {
		return new ByteArrayInputStream(s.toString().getBytes());
	}
}
