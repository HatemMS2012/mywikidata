package hms.wikidata.property;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import hms.wikidata.dbimport.JacksonDBAPI;
import hms.wikidata.model.PropertyOfficialCategory;
import hms.wikidata.model.StructuralPropertyMapper;

/**
 * This class provied statistic about properties such as their frequency, structure, etc.
 * @author mousselly
 *
 */
public class PropertyAnalayzer {
	
	
	
	
	//For properties of a given category generate statistics about their
	//structure-related items
	
	public static Map<String, Integer> analyze(PropertyOfficialCategory category){
		
		Map<String, Integer> statisticTabel = new HashMap<String, Integer>();
	
		List<String> officalProperties =  JacksonDBAPI.getOfficialProperties(category);
		
		Set<String> officalPropertiesSturctCount =  new HashSet<String>();
		

		for(Entry<String, String> property : StructuralPropertyMapper.structuarlPropertiesMap.entrySet()){
			
			String structProperty = property.getKey();
			String structPropertyLabel = JacksonDBAPI.getItemLabel(structProperty,"en");
			if(structPropertyLabel == null){
				structPropertyLabel = "Wikidata property example";
			}
			
			for(String officialProperty : officalProperties){
				
			
					
				List<String> range = JacksonDBAPI.getClaimRange(officialProperty,structProperty );
					
				
				if(range.size()> 0){
					
					officalPropertiesSturctCount.add(officialProperty);
						
					if(statisticTabel.get(structPropertyLabel)!=null){

						int count = statisticTabel.get(structPropertyLabel) +1 ;

						statisticTabel.put(structPropertyLabel, count);
					}
					else{
						statisticTabel.put(structPropertyLabel, 1);
					}
						
				}
					
			}
			if(statisticTabel.get(structPropertyLabel)==null){
				statisticTabel.put(structPropertyLabel, 0);

			}
			
			
		}
		int totalPropWithoutStructure =  officalProperties.size() - officalPropertiesSturctCount.size();
		printStaticTable(category, officalProperties.size(), totalPropWithoutStructure, statisticTabel);
		
		return statisticTabel;
	}
	
	public static void printStaticTable(PropertyOfficialCategory category , int total, int totalNoStruct, Map<String, Integer> table){
		
		System.out.println("Category \t" + category);
		System.out.println("Total \t" + total);
		System.out.println("Structural Property \t Count (" +category + ") \t % (" +category + ")");
		
		
		for(Entry<String, Integer> entry : table.entrySet()){
			int count = entry.getValue();
			System.out.println(entry.getKey() + " \t " + entry.getValue()  + " \t " + (double)count/(double)total);
		}
		System.out.println("NA" + " \t " + totalNoStruct  + " \t " + (double)totalNoStruct/(double)total);
	}
	
	public static void main(String[] args) {
		
		for (PropertyOfficialCategory type : PropertyOfficialCategory.values()) {
			analyze(type);
			System.out.println("");
		}
		
	}

}
