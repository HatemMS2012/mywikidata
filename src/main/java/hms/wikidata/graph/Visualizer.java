package hms.wikidata.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class Visualizer {


	private static StringBuffer result = new StringBuffer();
	
	public static void parse(String json) {
	
		
		JSONObject jObject = new JSONObject(json);

		String root = jObject.keys().next();
	    
	    Object nestedJSON = jObject.get(root);
	    
	    if(nestedJSON instanceof JSONObject){
	    	JSONObject jsonObj = (JSONObject) nestedJSON;
	    	Iterator<String> iter = jsonObj.keys();
	    	while(iter.hasNext()){
	    		String key = iter.next();
	    		String relationName = key.substring(key.indexOf("(")+ 1, key.indexOf(")")) ;
	    		
	    		String target = key.substring(key.indexOf("|")+1, key.lastIndexOf("|")); 
	    		
		    	System.out.println(root + "\t" +relationName + "\t" + target );	
		    	
		    	result.append(root + "\t" +relationName + "\t" + target ).append("\n");
		    	
		    	Object aa =  jsonObj.get(key);
		    	
	    		traverse(key,aa);
	    	}
	    }
	    
	    else if(nestedJSON instanceof JSONArray){ 
    		
			JSONArray aaa = (JSONArray) nestedJSON;
    	

			for (int i = 0; i < aaa.length(); i++) {
				
				String fullTarget = aaa.get(i).toString();
				
				String target = fullTarget.substring(fullTarget.indexOf("|")+1, fullTarget.lastIndexOf("|"));
				
	    		String relationName = fullTarget.substring(fullTarget.indexOf("(")+ 1, fullTarget.indexOf(")")) ;

	    		result.append(root + "\t" + relationName + "\t" + target ).append("\n");	
			}
		}
	}
	    

		public static void parseJSON4D3(String json) {
		
			
			JSONObject jObject = new JSONObject(json);

			String root = jObject.keys().next();
		    
		    Object nestedJSON = jObject.get(root);
		    
			result.append("{\"name\": \"").append(root).append("\"").append(",\"children\" : [\n");

		    if(nestedJSON instanceof JSONObject){
		    	JSONObject jsonObj = (JSONObject) nestedJSON;
		    	Iterator<String> iter = jsonObj.keys();
		    	while(iter.hasNext()){
		    		String key = iter.next();
		    		
		    		String relationName = key.substring(key.indexOf("(")+ 1, key.indexOf(")")) ;
		    		
		    		String target = key.substring(key.indexOf("|")+1, key.lastIndexOf("|")); 
		    		
//		    		System.out.println("-----------: " + root);
			    	
//			    	result.append("{\"name\": \"").append(key).append("\"").append(",\"children\" : [\n");//.append("\"name\":\"").append(target ).append("\"\n [{");
			    	
			    	Object aa =  jsonObj.get(key);
			    	

			    		traverseJSON4d3(key,aa);

		    	}
		    }
			else if(nestedJSON instanceof JSONArray){ 
	    		
	    		int nrCommas = ((JSONArray) nestedJSON).length()-1;
		    	int commmaCounter = 0;
	    		
				JSONArray aaa = (JSONArray) nestedJSON;
	    	

		    	
				for (int i = 0; i < aaa.length(); i++) {
					
					String fullTarget = aaa.get(i).toString();
					
					String target = fullTarget.substring(fullTarget.indexOf("|")+1, fullTarget.lastIndexOf("|"));
					
		    		String relationName = fullTarget.substring(fullTarget.indexOf("(")+ 1, fullTarget.indexOf(")")) ;


		    		
		    		result.append("{\"name\":\"").append(target ).append("\"}");
		    		
		    		if(commmaCounter < nrCommas){
		    			result.append(",\n");
		    			commmaCounter++;
		    		}
			    	
				}

			}
		    result.append("]}");


	}
	public static void traverseJSON4d3(String father, Object root) {
		
		String fatherLabel = father.substring(father.indexOf("|")+1, father.lastIndexOf("|")); 

		result.append("{\"name\":\"").append(fatherLabel + "\", \"children\" : [\n");
		
		if(root instanceof JSONObject) {
	    	Iterator<String> iter = ((JSONObject) root).keys();
	    	int nrCommas = ((JSONObject) root).length()-1;
	    	int commmaCounter = 0;

    	while(iter.hasNext()){
	    	
	    		String key = iter.next();
	    		
	    	
	    		String relationName = key.substring(key.indexOf("(")+ 1, key.indexOf(")")) ;
	    		
	    		String target = key.substring(key.indexOf("|")+1, key.lastIndexOf("|")); 
	    		

		    	
	    		Object aa =  ((JSONObject) root).get(key);
		    	
		    	traverseJSON4d3(key, aa);
		      
	
	    	}

	  
		}
    	else if(root instanceof JSONArray){ 
    		
    		int nrCommas = ((JSONArray) root).length()-1;
	    	int commmaCounter = 0;
    		
			JSONArray aaa = (JSONArray) root;
    	

	    	
			for (int i = 0; i < aaa.length(); i++) {
				
				String fullTarget = aaa.get(i).toString();
				
				String target = fullTarget.substring(fullTarget.indexOf("|")+1, fullTarget.lastIndexOf("|"));
				
	    		String relationName = fullTarget.substring(fullTarget.indexOf("(")+ 1, fullTarget.indexOf(")")) ;


	    		
	    		result.append("{\"name\":\"").append(target ).append("\"}");
	    		
	    		if(commmaCounter < nrCommas){
	    			result.append(",\n");
	    			commmaCounter++;
	    		}
		    	
			}

		}
		result.append("]},");
    	
	}
	
	public static void traverse(String father, Object root) {
		
		if(root instanceof JSONObject) {
	    	Iterator<String> iter = ((JSONObject) root).keys();
	    	
	    	while(iter.hasNext()){
	    	
	    		String key = iter.next();
	    		
	    		String fatherLabel = father.substring(father.indexOf("|")+1, father.lastIndexOf("|")); 
	    	
	    		String relationName = key.substring(key.indexOf("(")+ 1, key.indexOf(")")) ;
	    		
	    		String target = key.substring(key.indexOf("|")+1, key.lastIndexOf("|")); 
	    		
	    		result.append(fatherLabel + "\t" + relationName + "\t" + target ).append("\n");
		    	
	    		Object aa =  ((JSONObject) root).get(key);
		    	
		    	traverse(key, aa);
		  
	
	    	}
		}
    	else if(root instanceof JSONArray){ 
    		
			JSONArray aaa = (JSONArray) root;
    	
			String fatherLabel = father.substring(father.indexOf("|")+1, father.lastIndexOf("|")); 

			for (int i = 0; i < aaa.length(); i++) {
				
				String fullTarget = aaa.get(i).toString();
				
				String target = fullTarget.substring(fullTarget.indexOf("|")+1, fullTarget.lastIndexOf("|"));
				
	    		String relationName = fullTarget.substring(fullTarget.indexOf("(")+ 1, fullTarget.indexOf(")")) ;

	    		result.append(fatherLabel + "\t" + relationName + "\t" + target ).append("\n");	
			}
		}
	}
	
	
	public static String generateVisualationInstruction(String json){
		

		
		StringBuffer visCode = new StringBuffer();
		
		
		String[] resultLines = json.toString().split("\n");
		if(resultLines[0].length() == 0)
			return null;
		
		Map<String, Integer> nodeIdMap = new HashMap<String, Integer>();
		int id = 1 ;
		  
		visCode.append("var nodes = new vis.DataSet([\n");
		
		for(String line : resultLines){
			
			String[] linElement = line.split("\t");
			
			String sourceNode = linElement[0].replace("'", "");
		
			String targetNode = linElement[2].replace("'", "");

			
			if(nodeIdMap.get(sourceNode) == null){
				nodeIdMap.put(sourceNode, id);
				visCode.append(" {id:").append(id).append(", label: '").append(sourceNode).append("' },\n");
				id ++;
			}
			if(nodeIdMap.get(targetNode) == null){
				nodeIdMap.put(targetNode, id);
				visCode.append(" {id:").append(id).append(", label: '").append(targetNode).append("' },\n");
				id ++;
			}
			
			
			
			
		}
		visCode = new StringBuffer(visCode.toString().substring(0, visCode.toString().length()-2)); //remove the last comma

		visCode.append("]);\n \n \n");
		

		visCode.append( "var edges = new vis.DataSet([\n");

		for(String line : resultLines){
			
			String[] linElement = line.split("\t");
			String sourceNode = linElement[0].replace("'", "");;
			String relation = linElement[1].replace("'", "");;
			String targetNode = linElement[2].replace("'", "");;
			
			visCode.append("{from:").append(nodeIdMap.get(sourceNode)).append(", to:").append(nodeIdMap.get(targetNode)).append(",")
			.append("label:'").append(relation).append("',")
			.append("arrows: 'to'},").append("\n");
		}
		visCode = new StringBuffer(visCode.toString().substring(0, visCode.toString().length()-2)); //remove the last comma
		visCode.append(  "]);\n \n \n");

		return visCode.toString();
	}
	
	public static void main(String[] args) throws JSONException {
			
			String itemId = "Q1";
			int depth = 1;
			String lang = "en";
		
			List<String> targetProperties = new ArrayList<String>();
//			targetProperties.add("P279");
			targetProperties.add("P314");
//			targetProperties.add("P361");
//			String repsonse = Visualizer.generateTreeForEntity(itemId, depth, lang, targetProperties);
//			System.out.println(repsonse);
			
			String res = generateCodeForVis(itemId, depth, lang, targetProperties);
			System.out.println(res);
//			System.out.println("Output \n" + VIS_SPECIFIC_HEAD + "\n" +  res + VIS_SPECIFIC_TAIL );
			
	}

	 
	/**
	 * Generate d3 json response for a tree representation of wikidata entity (Q and P)
	 * @param itemId
	 * @param depth
	 * @param lang
	 * @param targetProperties Properties to consider in the construction of the tree
	 * @return
	 */
	public static String generateTreeForEntity(String itemId,int depth,String lang,	List<String> targetProperties  ){
		result = new StringBuffer();
		String json = WikidataTraverser.geneateTreeJSON(itemId,depth,lang,targetProperties);
		parseJSON4D3(json);
		String finalJSONString = result.toString().replace("},]", "}]");
		
		result = new StringBuffer();
		return finalJSONString;
		
	}
	
	public static String generateTreeAsText(String itemId,int depth,String lang,	List<String> targetProperties  ){
		result = new StringBuffer();
		String json = WikidataTraverser.geneateTreeJSON(itemId,depth,lang,targetProperties);
		parse(json);
		String finalJSONString = result.toString().replace("},]", "}]");
		return finalJSONString;
		
	}
	
	/**
	 * Generate d3 json response for a tree representation of wikidata entity (Q and P)
	 * @param itemId
	 * @param depth
	 * @param lang
	 * @param targetProperties Properties to consider in the construction of the tree
	 * @return
	 */
	public static String generateCodeForVis(String itemId,int depth,String lang,	List<String> targetProperties  ){
		result = new StringBuffer();
		
		//String json = WikidataTraverser.geneateTreeJSON(itemId,depth,lang,targetProperties);
		generateTreeAsText(itemId, depth, lang, targetProperties);
		System.out.println(result);
		String res= null;
		if(result.toString().length()!=0){
	
			 res = generateVisualationInstruction(result.toString());
		}
		
		
		return res;
		
	}
	
	
}
