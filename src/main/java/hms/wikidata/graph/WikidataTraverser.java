package hms.wikidata.graph;

import hms.wikidata.dbimport.JacksonDBAPI;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class WikidataTraverser {


	private static StringBuffer jsonOuptput = new StringBuffer();
	
	
	public static String geneateTreeJSON(String itemId, int depth,String lang, boolean advanced){
		
		if(advanced){
			generateTreeOverPropertiesAndValuesAdvanced(itemId, depth, lang);
		}
		else{
			generateTreeOverProperties(itemId, depth,lang);
		}
		
		String finalLabel = itemId;
		
		if(lang!=null){

			String itemLabel = JacksonDBAPI.getItemLabel(itemId,lang);

			finalLabel = itemId + " (" + itemLabel +  ")"; 
		}
		String finalString = "{\"" + finalLabel + "\":" + jsonOuptput.substring(0, jsonOuptput.toString().length()-1).toString() + "}";
		
		return finalString;
		
	}
	
	/**
	 * Generates a tree representation for a given Wikidata entity.
	 * The method recursively extract the properties of a given entity and their ranges and apply the 
	 * same procedure on the ranges
	 * @param itemId The item you want to create the tree for
	 * @param depth The recursive depth
	 * @param lang language of the labels
	 * @param targetProp The set of properties you want to consider in the hierarchy
	 * @return JSON representation of the tree
	 */
	public static String geneateTreeJSON(String itemId, int depth,String lang, List<String>targetProps){
		
	
		generateTreeOverPropertiesAndValues(itemId, depth, lang,targetProps);
		
		
		String finalLabel = itemId;
		
		if(lang!=null){

			String itemLabel = JacksonDBAPI.getItemLabel(itemId,lang);

			finalLabel = itemId + " (" + itemLabel +  ")"; 
		}
		String finalString = "{\"" + finalLabel + "\":" + jsonOuptput.substring(0, jsonOuptput.toString().length()-1).toString() + "}";
		
		return finalString;
		
	}
	
	public static String geneateTreeJSON2(String itemId, int depth,String lang, List<String>targetProps){
		
		
		generateTreeOverPropertiesAndValues2(itemId, depth, lang,targetProps);
		
		
		String finalLabel = itemId;
		
		if(lang!=null){

			String itemLabel = JacksonDBAPI.getItemLabel(itemId,lang);

			finalLabel = itemId + " (" + itemLabel +  ")"; 
		}
		String finalString = "{\"name\": \"" + finalLabel + "\", \"children\":" + jsonOuptput.substring(0, jsonOuptput.toString().length()-1).toString() + "}";
		
		return finalString;
		
	}
	
	/**
	 * Generates a tree representation for a given wikidata entity
	 * @param itemId
	 * @param depth
	 * @param lang
	 */
	public static void generateTreeOverProperties(String itemId, int depth, String lang){
		if(depth <= 0)
			
			return;
		
		Set<String> claimList = new HashSet<String>(JacksonDBAPI.getEntityClaimsIds(itemId));
		
		if(claimList.size() == 0){
			jsonOuptput.append("[],");
			return;
		}
		
		depth--;
		
		if(depth==0){
			jsonOuptput.append("[");
		}
		else{
			jsonOuptput.append("{");
		}

		int nrCommas = claimList.size()-1 ;
		int countCommas = 1;
		
		for(String claimId : claimList){
			
			String labels = claimId;
			if(lang !=null){
				String claimLabel = JacksonDBAPI.getItemLabel(claimId,lang);
				labels = claimId + " (" +claimLabel+")" ; 
			}
			
			
			jsonOuptput.append("\"").append(labels).append("\"");
			
			if(depth!=0){
				jsonOuptput.append(":");
			}
			else if(countCommas <= nrCommas){
				jsonOuptput.append(",");
				countCommas ++ ;
			}
		
			generateTreeOverProperties(claimId,depth,lang);
			
		}
		if(depth==0 ){
			jsonOuptput.append("],");
		}
		
		else{
			
			jsonOuptput.append("},");
			jsonOuptput = new StringBuffer(jsonOuptput.toString().replace("],}", "]}").replace("},},", "}},"));
		

		}
	}
	
	/**
	 * Generates a tree representation for a given wikidata entity and output the labels 
	 * @param itemId
	 * @param depth
	 * @param lang
	 */
	public static void generateTreeOverPropertiesAdvanced(String itemId, int depth, String lang){
		if(depth <= 0)
			
			return;
		
		Map<String, String> claimValueMap = JacksonDBAPI.getEntityClaimsIdsAndValues(itemId);
		
		if(claimValueMap.size() == 0){
			jsonOuptput.append("[],");
			return;
		}
		
		depth--;
		
		if(depth==0){
			jsonOuptput.append("[");
		}
		else{
			jsonOuptput.append("{");
		}

		int nrCommas = claimValueMap.size()-1 ;
		int countCommas = 1;
		
		for(String claimId : claimValueMap.keySet()){
			
			String labels = claimId;
			
			if(lang !=null){
				String claimLabel = JacksonDBAPI.getItemLabel(claimId,lang);
				labels = claimId + " (" +claimLabel+")" ;
				String claimVal = claimValueMap.get(claimId);
				if(claimVal.startsWith("Q")){
					 String claimValLab = JacksonDBAPI.getItemLabel(claimVal,lang);
					 claimVal = claimVal +": " +claimValLab;
				}
				
				labels += " |" + claimVal + "|";
			}
			
			
			jsonOuptput.append("\"").append(labels).append("\"");
			
			if(depth!=0){
				jsonOuptput.append(":");
			}
			else if(countCommas <= nrCommas){
				jsonOuptput.append(",");
				countCommas ++ ;
			}
		
			generateTreeOverPropertiesAdvanced(claimId,depth,lang);
			
		}
		if(depth==0 ){
			jsonOuptput.append("],");
		}
		
		else{
			
			jsonOuptput.append("},");
			jsonOuptput = new StringBuffer(jsonOuptput.toString().replace("],}", "]}").replace("},},", "}},"));
		

		}
	}
	/**
	 * Generates a tree representation for a given wikidata entity
	 * @param itemId
	 * @param depth
	 * @param lang
	 */
	public static void generateTreeOverPropertiesAndValuesAdvanced(String itemId, int depth, String lang){
		if(depth <= 0)
			
			return;
		
		Map<String, String> claimValueMap = JacksonDBAPI.getEntityClaimsIdsAndValues(itemId);
		
		if(claimValueMap.size() == 0){
			jsonOuptput.append("[],");
			return;
		}
		
		depth--;
		
		if(depth==0){
			jsonOuptput.append("[");
		}
		else{
			jsonOuptput.append("{");
		}

		int nrCommas = claimValueMap.size()-1 ;
		int countCommas = 1;
		
		for(String claimId : claimValueMap.keySet()){
			
			String labels = claimId;
			String claimValMain = claimValueMap.get(claimId);
			if(lang !=null){
				String claimLabel = JacksonDBAPI.getItemLabel(claimId,lang);
				labels = claimId + " (" +claimLabel+")" ;
				String claimVal = claimValueMap.get(claimId);
				if(claimVal.startsWith("Q")){
					 String claimValLab = JacksonDBAPI.getItemLabel(claimVal,lang);
					 claimVal = claimVal +": " +claimValLab;
				}
				
				labels += " |" + claimVal + "|";
			}
			
			
			jsonOuptput.append("\"").append(labels).append("\"");
			
			if(depth!=0){
				jsonOuptput.append(":");
			}
			else if(countCommas <= nrCommas){
				jsonOuptput.append(",");
				countCommas ++ ;
			}
		
			generateTreeOverPropertiesAndValuesAdvanced(claimValMain,depth,lang);
			
		}
		if(depth==0 ){
			jsonOuptput.append("],");
		}
		
		else{
			
			jsonOuptput.append("},");
			jsonOuptput = new StringBuffer(jsonOuptput.toString().replace("],}", "]}").replace("},},", "}},"));
		

		}
	}
	
	/**
	 * Generates a tree representation for a given wikidata entity.
	 * The method recursively extract the properties of a given entity and their ranges and apply the 
	 * same procedure on the ranges
	 * @param itemId The item you want to create the tree for
	 * @param depth The recursive depth
	 * @param lang language of the labels
	 * @param targetProp The set of properties you want to consider in the hierarchy
	 */
	private static void generateTreeOverPropertiesAndValues(String itemId, int depth, String lang, List<String> targetProp){
		if(depth <= 0)
			
			return;
		
		Map<String, String> claimValueMap = JacksonDBAPI.getEntityClaimsIdsAndValues(itemId);
		
		if(claimValueMap.size() == 0){
			jsonOuptput.append("[],");
			return;
		}
		
		int nrCommas = 0 ;
		//count the total of accepted elements
		for(String claimId : claimValueMap.keySet()){
			if(targetProp.contains(claimId.split("#")[0])){
				nrCommas++;
			}
		}
		
		depth--;
		
		if(depth==0){
			jsonOuptput.append("[");
		}
		else{
			jsonOuptput.append("{");
		}

	
		int countCommas = 1;
		
		for(String claimId : claimValueMap.keySet()){
			
			String claimIdWithouthSource = claimId.split("#")[0];
			
			if(targetProp.contains(claimIdWithouthSource)){
				
				String labels = claimId;
				String claimValMain = claimValueMap.get(claimId);
			
				if(lang !=null){
			
					String claimLabel = JacksonDBAPI.getItemLabel(claimIdWithouthSource,lang);
				
					labels = claimIdWithouthSource + " (" +claimLabel+")" ;
				
					String claimVal = claimValueMap.get(claimId);
					
					if(claimVal.startsWith("Q")){
						
						 String claimValLab = JacksonDBAPI.getItemLabel(claimVal,lang);
						 
						 claimVal = claimVal +": " +claimValLab;
					}
					
					labels += " |" + claimVal + "|";
				}
				
				
				jsonOuptput.append("\"").append(labels).append("\"");
				
				if(depth!=0){
					jsonOuptput.append(":");
				}
				else if(countCommas < nrCommas){
					jsonOuptput.append(",");
					countCommas ++ ;
				}
			
				generateTreeOverPropertiesAndValues(claimValMain,depth,lang,targetProp);
			}
		}
		if(depth==0 ){
			jsonOuptput.append("],");
		}
		
		else{
			
			jsonOuptput.append("},");
			jsonOuptput = new StringBuffer(jsonOuptput.toString().replace("],}", "]}").replace("},},", "}},"));
		

		}
	}
	

private static void generateTreeOverPropertiesAndValues2(String itemId, int depth, String lang, List<String> targetProp){
		if(depth <= 0)
			
			return;
		
		Map<String, String> claimValueMap = JacksonDBAPI.getEntityClaimsIdsAndValues(itemId);
		
		if(claimValueMap.size() == 0){
			jsonOuptput.append("{},");
			return;
		}
		
		int nrCommas = 0 ;
		//count the total of accepted elements
		for(String claimId : claimValueMap.keySet()){
			if(targetProp.contains(claimId.split("#")[0])){
				nrCommas++;
			}
		}
		
		depth--;
		
		if(depth==0){
			jsonOuptput.append("{");
		}
		else{
			jsonOuptput.append("[{");
		}

	
		int countCommas = 1;
		
		for(String claimId : claimValueMap.keySet()){
			
			String claimIdWithouthSource = claimId.split("#")[0];
			
			if(targetProp.contains(claimIdWithouthSource)){
				
				String labels = claimId;
				String claimValMain = claimValueMap.get(claimId);
			
				if(lang !=null){
			
					String claimLabel = JacksonDBAPI.getItemLabel(claimIdWithouthSource,lang);
				
					labels = "name\": \""+ claimIdWithouthSource + " (" +claimLabel+")" ;
				
					String claimVal = claimValueMap.get(claimId);
					
					if(claimVal.startsWith("Q")){
						
						 String claimValLab = JacksonDBAPI.getItemLabel(claimVal,lang);
						 
						 claimVal = claimVal +": " +claimValLab;
					}
					
					labels += " |" + claimVal + "|";
					
					
				}
				

				
				if(depth!=0){
					jsonOuptput.append(", \"children\":");
				}
				else if(countCommas < nrCommas){
					jsonOuptput.append(",");
					countCommas ++ ;
				}
				
			
				generateTreeOverPropertiesAndValues(claimValMain,depth,lang,targetProp);
			}
		}
		if(depth==0 ){
			jsonOuptput.append("},");
		}
		
		else{
			
			jsonOuptput.append("}],");
//			jsonOuptput = new StringBuffer(jsonOuptput.toString().replace("},]", "}]").replace("],],", "]],"));
			jsonOuptput = new StringBuffer(jsonOuptput.toString().replace("],}", "}]"));
		

		}
	}
	
	public static void main(String[] args) {
		String itemId = "Q1";
		int depth = 3;
		String lang = "en";
	
//		System.out.println(geneateTreeJSON(itemId,depth,lang,false));
//		System.out.println(geneateTreeJSON(itemId,depth,lang,true));

		
		List<String> targetProperties = new ArrayList<String>();
		targetProperties.add("P279");
		targetProperties.add("P31");
		targetProperties.add("P361");
		
		
		System.out.println(geneateTreeJSON(itemId,depth,lang,targetProperties));
	}
	
	
}
