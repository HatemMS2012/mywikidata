package hms.wikidata.model;

import java.util.HashMap;
import java.util.Map;

public class StructuralPropertyMapper {

	public static String inverseOf = "P1696";
	public static  String equivalentProperty = "P1628" ;
	public  static String subPropertyOf ="P1647";
	public  static String propExample ="P1855";
	//used to indicate a property that might provide additional information about the subject
	public  static String seeAlso = "P1659";
	public  static String subjectItem = "P1629";
	public  static String mandatoryQualifier = "P1646";
	
	
	//Factual
	public  static String instanceOf = "P31";
	public  static String subclassOf = "P279";
	public  static String facetOf = "P1269";
	public  static String partOf = "P361" ;
	public  static String hasPart = "P527" ;
	
	
	public static Map<String, String> structuarlPropertiesMap = new HashMap<String, String>();
	public static Map<String, String> structuarlItemRelatedPropertiesMap = new HashMap<String, String>();

	static{
		
		structuarlPropertiesMap.put( "P1696", "inverse of");
		structuarlPropertiesMap.put("P1628", "equivalentProperty");
		structuarlPropertiesMap.put("P1647", "subPropertyOf");
		structuarlPropertiesMap.put("P1855", "propExample");
		structuarlPropertiesMap.put("P1659","seeAlso" );
		structuarlPropertiesMap.put("P1629", "subjectItem");
		structuarlPropertiesMap.put("P1646", "mandatoryQualifier");
		structuarlPropertiesMap.put("P31", "instanceOf");
	}
	static{
		
		structuarlItemRelatedPropertiesMap.put("P279", "subclassOf");
		structuarlItemRelatedPropertiesMap.put("P31", "instanceOf");
	}
}
