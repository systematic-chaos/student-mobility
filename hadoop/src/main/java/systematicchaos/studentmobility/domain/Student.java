/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - domain/Student.java
 */

package systematicchaos.studentmobility.domain;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Locale;

public class Student {
	
    private String homeInstitution;
    private String countryOfHomeInstitution;
    private Integer age;
    private Gender gender;
    private StudyLevel studyLevel;
    private MobilityType mobilityType;
    private String hostInstitution;
    
    private String countryOfHostInstitution;
    private String countryOfWorkPlacement;
    private Float lengthStudyPeriod;
    private Float lengthWorkPlacement;
    private LocalDate studyStartDate;
    private LocalDate placementStartDate;
    private Boolean taughtHostLang;
    private String languageTaught;
    private Float studyGrant;
    private Float placementGrant;
    
    private static final String EMPTY_STRING = "";
    private static final String DEFAULT_STR = EMPTY_STRING;
    private static final int ZERO = 0;
    private static final int DEFAULT_INT = ZERO;
    private static final boolean DEFAULT_BOOL = Boolean.FALSE;
    private static final String SPLIT = String.valueOf(';');
    
    private static final int NUM_ATTRIBUTES_BASIC = 8;
    private static final int NUM_ATTRIBUTES_FULL = 17;
    
    public Student() {
    }

    /**
     * Constructor with parameters just for compulsory attributes
     */
    public Student(String homeInstitution, String countryOfHomeInstitution,
            Integer age, Gender gender, StudyLevel studyLevel, MobilityType mobilityType,
            String hostInstitution, String countryOfHostInstitution) {
    	this.setHomeInstitution(homeInstitution);
    	this.setCountryOfHomeInstitution(countryOfHomeInstitution);
    	this.setAge(age);
    	this.setGender(gender);
    	this.setStudyLevel(studyLevel);
    	this.setMobilityType(mobilityType);
    	this.setHostInstitution(hostInstitution);
    	this.setCountryOfHostInstitution(countryOfHostInstitution);
    }

    /**
     * Constructor with parameters for all attributes
     */
    public Student(String homeInstitution, String countryOfHomeInstitution,
            Integer age, Gender gender, StudyLevel studyLevel, MobilityType mobilityType,
            String hostInstitution, String countryOfHostInstitution, String countryOfWorkPlacement,
            Float lengthStudyPeriod, Float lengthWorkPlacement,
            LocalDate studyStartDate, LocalDate placementStartDate,
            Boolean taughtHostLang, String languageTaught,
            Float studyGrant, Float placementGrant) {
        this(homeInstitution, countryOfHomeInstitution, age, gender, studyLevel,
        		mobilityType, hostInstitution, countryOfHostInstitution);
        this.setCountryOfWorkPlacement(countryOfWorkPlacement);
        this.setLengthStudyPeriod(lengthStudyPeriod);
        this.setLengthWorkPlacement(lengthWorkPlacement);
        this.setStudyStartDate(studyStartDate);
        this.setPlacementStartDate(placementStartDate);
        this.setTaughtHostLang(taughtHostLang);
        this.setLanguageTaught(languageTaught);
        this.setStudyGrant(studyGrant);
        this.setPlacementGrant(placementGrant);
    }
    
    public String getHomeInstitution() {
    	return this.homeInstitution;
    }
    
    public void setHomeInstitution(String homeInstitution) {
    	this.homeInstitution = homeInstitution;
    }
    
    public String getCountryOfHomeInstitution() {
    	return this.countryOfHomeInstitution;
    }
    
    public void setCountryOfHomeInstitution(String countryOfHomeInstitution) {
    	this.countryOfHomeInstitution = countryOfHomeInstitution;
    }
    
    public Integer getAge() {
    	return this.age;
    }
    
    public void setAge(Integer age) {
    	this.age = age;
    }
    
    public Gender getGender() {
    	return this.gender;
    }
    
    public void setGender(Gender gender) {
    	this.gender = gender;
    }
    
    public StudyLevel getStudyLevel() {
    	return this.studyLevel;
    }
    
    public void setStudyLevel(StudyLevel studyLevel) {
    	this.studyLevel = studyLevel;
    }
    
    public MobilityType getMobilityType() {
    	return this.mobilityType;
    }
    
    public void setMobilityType(MobilityType mobilityType) {
    	this.mobilityType = mobilityType;
    }
    
    public String getHostInstitution() {
    	return this.hostInstitution;
    }
    
    public void setHostInstitution(String hostInstitution) {
    	this.hostInstitution = hostInstitution;
    }
    
    public String getCountryOfHostInstitution() {
    	return this.countryOfHostInstitution;
    }
    
    public void setCountryOfHostInstitution(String countryOfHostInstitution) {
    	this.countryOfHostInstitution = countryOfHostInstitution;
    }
    
    public String getCountryOfWorkPlacement() {
    	return this.countryOfWorkPlacement;
    }
    
    public void setCountryOfWorkPlacement(String countryOfWorkPlacement) {
    	this.countryOfWorkPlacement = countryOfWorkPlacement;
    }
    
    public Float getLengthStudyPeriod() {
    	return this.lengthStudyPeriod;
    }
    
    public void setLengthStudyPeriod(Float lengthStudyPeriod) {
    	this.lengthStudyPeriod = lengthStudyPeriod;
    }
    
    public Float getLengthWorkPlacement() {
    	return this.lengthWorkPlacement;
    }
    
    public void setLengthWorkPlacement(Float lengthWorkPlacement) {
    	this.lengthWorkPlacement = lengthWorkPlacement;
    }
    
    public LocalDate getStudyStartDate() {
    	return this.studyStartDate;
    }
    
    public void setStudyStartDate(LocalDate studyStartDate) {
    	this.studyStartDate = studyStartDate;
    }
    
    public LocalDate getPlacementStartDate() {
    	return this.placementStartDate;
    }
    
    public void setPlacementStartDate(LocalDate placementStartDate) {
    	this.placementStartDate = placementStartDate;
    }
    
    public Boolean getTaughtHostLang() {
    	return this.taughtHostLang;
    }
    
    public void setTaughtHostLang(Boolean taughtHostLang) {
    	this.taughtHostLang = taughtHostLang;
    }
    
    public String getLanguageTaught() {
    	return this.languageTaught;
    }
    
    public void setLanguageTaught(String languageTaught) {
    	this.languageTaught = languageTaught;
    }
    
    public Float getStudyGrant() {
    	return this.studyGrant;
    }
    
    public void setStudyGrant(Float studyGrant) {
    	this.studyGrant = studyGrant;
    }
    
    public Float getPlacementGrant() {
    	return this.placementGrant;
    }
    
    public void setPlacementGrant(Float placementGrant) {
    	this.placementGrant = placementGrant;
    }

    /**
     * Returns a string with all the Student properties joined by semicolons.
     * It applies default values to some attributes if they are missing.
     * The format of the string returned is analogous to the one admitted by the
     * @code{fromString(String)} method.
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        
        str.append(this.getHomeInstitution()).append(SPLIT);
        str.append(this.getCountryOfHomeInstitution()).append(SPLIT);
        str.append(this.getAge()).append(SPLIT);
        str.append(this.getGender()).append(SPLIT);
        str.append(this.getStudyLevel()).append(SPLIT);
        str.append(this.getMobilityType()).append(SPLIT);
        str.append(this.getHostInstitution());
        
        String countryOfHostInstitution = this.getCountryOfHostInstitution();
        String countryOfWorkPlacement = this.getCountryOfWorkPlacement();
        Float lengthStudyPeriod = this.getLengthStudyPeriod();
        Float lengthWorkPlacement = this.getLengthWorkPlacement();
        LocalDate studyStartDate = this.getStudyStartDate();
        LocalDate placementStartDate = this.getPlacementStartDate();
        Boolean taughtHostLang = this.getTaughtHostLang();
        String languageTaught = this.getLanguageTaught();
        Float studyGrant = this.getStudyGrant();
        Float placementGrant = this.getPlacementGrant();

        str.append(SPLIT).append(countryOfHostInstitution != null ? countryOfHostInstitution : DEFAULT_STR);
        str.append(SPLIT).append(countryOfWorkPlacement != null ? countryOfWorkPlacement : DEFAULT_STR);
        str.append(SPLIT).append(String.valueOf(lengthStudyPeriod != null ? lengthStudyPeriod : DEFAULT_INT));
        str.append(SPLIT).append(String.valueOf(lengthWorkPlacement != null ? lengthWorkPlacement : DEFAULT_INT));
        str.append(SPLIT).append(studyStartDate != null ? studyStartDate.toString() : DEFAULT_STR);
        str.append(SPLIT).append(placementStartDate != null ? placementStartDate.toString() : DEFAULT_STR);
        str.append(SPLIT).append(String.valueOf(taughtHostLang != null ? taughtHostLang : DEFAULT_BOOL));
        str.append(SPLIT).append(languageTaught != null ? languageTaught : DEFAULT_STR);
        str.append(SPLIT).append(String.valueOf(studyGrant != null ? studyGrant : DEFAULT_INT));
        str.append(SPLIT).append(String.valueOf(placementGrant != null ? placementGrant : DEFAULT_INT));
        
        return str.toString();
    }
    
    /**
     * Convenience method for building a Student instance from a semicolon separated string
     * containing the object's properties.
     */
    public static Student fromString(String stringifiedStudent) throws StudentParseException {
    	Student student;
    	int currentField = 0;
    	String[] studentFields = stringifiedStudent.split(SPLIT);
    	
    	// Neither basic nor full student
    	if (studentFields.length < NUM_ATTRIBUTES_BASIC - 1
    			|| studentFields.length > NUM_ATTRIBUTES_FULL) {
    		System.err.println(String.format("%s: %s",
    				StudentParseException.class.getName(),
    				StudentParseException.forInputString(stringifiedStudent).getMessage()));
    		return new Student();
    	}
    	
    	try {
	    	String homeInstitution = studentFields[currentField];
	    	String countryOfHomeInstitution = studentFields[++currentField];
	    	Integer age = studentFields[++currentField].length() > 0 ? Integer.valueOf(studentFields[currentField]) : null;
	    	Gender gender = Gender.fromValue(studentFields[++currentField].charAt(0));
	    	StudyLevel studyLevel = StudyLevel.fromValue(studentFields[++currentField].charAt(0));
	    	MobilityType mobilityType = MobilityType.fromValue(studentFields[++currentField].charAt(0));
	    	String hostInstitution = studentFields[++currentField];
	    	String countryOfHostInstitution = studentFields.length >= NUM_ATTRIBUTES_BASIC ? studentFields[++currentField] : EMPTY_STRING;
	    	
	    	if (studentFields.length <= NUM_ATTRIBUTES_BASIC) {
	    		student = new Student(homeInstitution, countryOfHomeInstitution,
	    				age, gender, studyLevel, mobilityType, hostInstitution, countryOfHostInstitution);
	    	} else {
	    		String countryOfWorkPlacement = studentFields[++currentField];
	    		Float lengthStudyPeriod = studentFields[++currentField].length() > 0 ? parseFloat(studentFields[currentField]) : null;
	    		Float lengthWorkPlacement = studentFields[++currentField].length() > 0 ? parseFloat(studentFields[currentField]) : null;
	    		LocalDate studyStartDate = dateFromString(studentFields[++currentField]);
	    		LocalDate placementStartDate = dateFromString(studentFields[++currentField]);
	    		Boolean taughtHostLang = boolFromChar(studentFields[++currentField]);
	    		
	    		String languageTaught = studentFields[++currentField];
	    		Float studyGrant = studentFields.length > ++currentField && studentFields[currentField].length() > 0 ?
	    				parseFloat(studentFields[currentField]) : null;
	    		Float placementGrant = studentFields.length > ++currentField && studentFields[currentField].length() > 0 ?
	    				parseFloat(studentFields[currentField]) : null;
	    		
	    		student = new Student(homeInstitution, countryOfHomeInstitution,
	    				age, gender, studyLevel, mobilityType, hostInstitution,
	    				countryOfHostInstitution, countryOfWorkPlacement,
	    				lengthStudyPeriod, lengthWorkPlacement,
	    				studyStartDate, placementStartDate, taughtHostLang,
	    				languageTaught, studyGrant, placementGrant);
	    	}
    	} catch (Exception e) {
    		System.err.println(stringifiedStudent);
    		System.err.println(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
    		return new Student();
    	}
    	
    	return student;
    }
    
    private static final char MALE_VALUE = 'M';
    private static final char FEMALE_VALUE = 'F';
    
    public static enum Gender {
    	
		MALE("MALE", MALE_VALUE),
		FEMALE("FEMALE", FEMALE_VALUE);
		
		private char value;
		private String name;
		
		Gender(String name, char value) {
			this.name = name;
			this.value = value;
		}
		
		public static Gender fromValue(char value) {
			Gender gender = null;
			switch (value) {
			case MALE_VALUE:
				gender = Gender.MALE;
				break;
			case FEMALE_VALUE:
				gender = Gender.FEMALE;
				break;
			}
			return gender;
		}
		
		public char getValue() {
			return this.value;
		}
		
		@Override
		public String toString() {
			return this.name;
		}
    }
    
    private static final char FIRST_VALUE = '1';
    private static final char SECOND_VALUE = '2';
    private static final char SUPERIOR_VALUE = 'S';
    
    public static enum StudyLevel {
    	
    	FIRST("FIRST", FIRST_VALUE),
    	SECOND("SECOND", SECOND_VALUE),
    	SUPERIOR("SUPERIOR", SUPERIOR_VALUE);
    	
    	private char value;
    	private String name;
    	
    	StudyLevel(String name, char value) {
    		this.name = name;
    		this.value = value;
    	}
    	
    	public static StudyLevel fromValue(char value) {
    		StudyLevel sl = null;
    		switch (value) {
    		case FIRST_VALUE:
    			sl = StudyLevel.FIRST;
    			break;
    		case SECOND_VALUE:
    			sl = StudyLevel.SECOND;
    			break;
    		case SUPERIOR_VALUE:
    			sl = StudyLevel.SUPERIOR;
    			break;
    		}
    		return sl;
    	}
    	
    	public char getValue() {
    		return this.value;
    	}
    	
    	@Override
    	public String toString() {
    		return this.name;
    	}
    }
    
    private static final char STUDY_VALUE = 'S';
    private static final char PLACEMENT_VALUE = 'P';
    private static final char OTHER_VALUE = 'C';
    
    public static enum MobilityType {
    	
    	STUDY("STUDY", STUDY_VALUE),
    	PLACEMENT("PLACEMENT", PLACEMENT_VALUE),
    	OTHER("OTHER", OTHER_VALUE);
    	
    	private char value;
    	private String name;
    	
    	MobilityType(String name, char value) {
    		this.name = name;
    		this.value = value;
    	}
    	
    	public static MobilityType fromValue(char value) {
    		MobilityType mt = null;
    		switch (value) {
    		case STUDY_VALUE:
    			mt = MobilityType.STUDY;
    			break;
    		case PLACEMENT_VALUE:
    			mt = MobilityType.PLACEMENT;
    			break;
    		case OTHER_VALUE:
    			mt = MobilityType.OTHER;
    			break;
    		}
    		return mt;
    	}
    	
    	public char getValue() {
    		return this.value;
    	}
    	
    	@Override
    	public String toString() {
    		return this.name;
    	}
    }
    
    private static Boolean boolFromChar(String input) {
    	final char YES = 'Y';
    	final char NO = 'N';
    	
    	boolean output = DEFAULT_BOOL;
    	if (input.length() > 0) {
    		switch (input.charAt(0)) {
    		case YES:
    			output = true;
    			break;
    		case NO:
    			output = false;
    			break;
    		}
		}
    	return output;
    }
    
    private static LocalDate dateFromString(String input) throws DateTimeParseException {
    	final char split = input.indexOf('-') > 0 ? '-' : '/';
    	LocalDate output = null;
    	String pattern = null;
    	
    	switch (input.length()) {
    	case 0:
    		break;
    	case 7:
    		if (!input.endsWith("0000")) {
	    		input = "1" + split + input;
	    		pattern = "d" + split + "MM" + split + "yyyy";
    		}
    		break;
    	case 20:
    		pattern = "dd-MMM-yyyy HH.mm.ss";
    		break;
    	}
    	
    	if (pattern != null) {
    		DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseCaseInsensitive()
    				.appendPattern(pattern).toFormatter(Locale.ENGLISH);
    		output = LocalDate.parse(input, formatter);
    	}
    	return output;
    }
    
    private static Float parseFloat(String input) throws NumberFormatException {
    	Float output = null;
    	if (input.length() > 0) {
    		input = input.replace(',', '.');
    		output = Float.valueOf(input);
    	}
    	return output;
    }
    
    public static class StudentParseException extends IllegalArgumentException {
    	
		private static final long serialVersionUID = 4604040086577779690L;

		public StudentParseException() {
    		super();
    	}
    	
    	public StudentParseException(String s) {
    		super(s);
    	}
    	
    	static StudentParseException forInputString(String s) {
    		return new StudentParseException("For input string: \"" + s + "\"");
    	}
    }
}
