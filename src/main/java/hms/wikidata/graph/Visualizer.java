package hms.wikidata.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;

public class Visualizer {


	static StringBuffer result = new StringBuffer();
	
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
		    	
//		    	if(aa instanceof JSONObject){
		    		traverse(key,aa);
//		    	}
//		    	else if(aa instanceof JSONArray){ 
//		    		JSONArray aaa = (JSONArray) aa;
//		    		for (int i = 0; i < aaa.length(); i++) {
//		    			System.out.println(root + ":" + aaa.get(i));	
//					}
//		    		
//		    	}
	    	}
	    }
	}
	    

		public static void parse2(String json) {
		
			
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
			    	

			    		traverse2(key,aa);

		    	}
		    }
		    result.append("]}");


	}
	public static void traverse2(String father, Object root) {
		
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
		    	
		    	traverse2(key, aa);
		      
	
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
	
	
	public static String generateVisualationInstruction(){
		

		
		StringBuffer visCode = new StringBuffer();
		
		
		String[] resultLines = result.toString().split("\n");
		
		Map<String, Integer> nodeIdMap = new HashMap<String, Integer>();
		int id = 1 ;
		
//		 // create an array with nodes
//		  var nodes = new vis.DataSet([
//		    {id: 1, label: 'Node 1'},
//		    {id: 2, label: 'Node 2'},
//		    {id: 3, label: 'Node 3'},
//		    {id: 4, label: 'Node 4'},
//		    {id: 5, label: 'Node 5'},
//		    {id: 6, label: 'Node 6'},
//		    {id: 7, label: 'Node 7'},
//		    {id: 8, label: 'Node 8'}
//		  ]);
//		  
		  
		visCode.append("var nodes = new vis.DataSet([\n");
		
		for(String line : resultLines){
			
			String[] linElement = line.split("\t");
			
			String sourceNode = linElement[0];
		
			String targetNode = linElement[2];

			
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
		
//		 var edges = new vis.DataSet([
//        {from: 1, to: 8, arrows:'to', dashes:true},
//        {from: 1, to: 3, arrows:'to'},
//        {from: 1, to: 2, arrows:'to, from'},
//        {from: 2, to: 4, arrows:'to, middle'},
//        {from: 2, to: 5, arrows:'to, middle, from'},
//        {from: 5, to: 6, arrows:{to:{scaleFactor:2}}},
//        {from: 6, to: 7, arrows:{middle:{scaleFactor:0.5},from:true}}
//      ]);
		visCode.append( "var edges = new vis.DataSet([\n");

		for(String line : resultLines){
			
			String[] linElement = line.split("\t");
			String sourceNode = linElement[0];
			String relation = linElement[1];
			String targetNode = linElement[2];
			
			visCode.append("{from:").append(nodeIdMap.get(sourceNode)).append(", to:").append(nodeIdMap.get(targetNode)).append(",")
			.append("label:'").append(relation).append("',")
			.append("arrows: 'to'},").append("\n");
		}
		visCode = new StringBuffer(visCode.toString().substring(0, visCode.toString().length()-2)); //remove the last comma
		visCode.append(  "]);\n \n \n");

		return visCode.toString();
	}
	
	public static void main(String[] args) throws JSONException {
			String itemId = "Q76";
			int depth = 6;
			String lang = "en";
		
			List<String> targetProperties = new ArrayList<String>();
			targetProperties.add("P279");
			targetProperties.add("P31");
			targetProperties.add("P361");
			
			
			String json = WikidataTraverser.geneateTreeJSON(itemId,depth,lang,targetProperties);
			System.out.println(json);
			parse2(json);
			
			System.out.println("........");
			System.out.println(result.toString().replace("},]", "}]"));
//			System.out.println(result.toString().replace("},}", "}}"));

			
//			System.out.println(generateVisualationInstruction());
//			
	}
	 
	 
	
//	public static void main(String[] args) throws JsonParseException, IOException {
//		
//		String itemId = "Q1";
//		int depth = 2;
//		String lang = "en";
//	
//		List<String> targetProperties = new ArrayList<String>();
//		targetProperties.add("P279");
//		targetProperties.add("P31");
//		targetProperties.add("P361");
//		
//		
//		String json = WikidataTraverser.geneateTreeJSON(itemId,depth,lang,targetProperties);
//		
//		generateGraphStructure(json);
//		
//	}
}
