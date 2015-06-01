package hms.wikidata.dbimport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JacksonDBAPI {

	
  //Queries
    
    private static final String SELECT_CLAIM_ARGUMENTS = "SELECT entity_id as domain, wikidata_item_value as \"range\" FROM wikidata_20150420.wiki_claim where claim_id =? limit ?" ;
    private static final String SELECT_ITEM_LABEL = "SELECT value FROM label where entity_id = ? and language =?";
    private static final String SELECT_ITEM_BY_VALUE = "SELECT * FROM label where value = ? and language =?";

    
    private static final String SELECT_ITEM_DESCRIPTION = "SELECT value FROM description where entity_id = ? and language =?";
    
    
    private static final String SELECT_PROPETY_IDS = "SELECT id FROM item WHERE type = 'property' " ;
    
    private static final String SELECT_PROPETY_CLAIM_COUNT = "SELECT count(*) as total FROM wikidata_20150420.wiki_claim where claim_id = ?";
    		
    private static final String SELECT_PROPETY_RANGE_TYPE = "SELECT type FROM wikidata_20150420.wiki_claim where claim_id = ? limit 1";

    private static final String SELECT_ITEM_ALIASES = "select * from alias where entity_id = ?  and language =?";

    
	  public static Map<String, String> getClaimArguments(String calimId, String lang, int maxReturned){
	      
	    	Map<String, String> argList = new HashMap<String, String>();
	    	PreparedStatement st;
			try {
				st =WikidataToRDB.conn.prepareStatement(SELECT_CLAIM_ARGUMENTS);
				st.setString(1, calimId);
				st.setInt(2, maxReturned);
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		
		    		String domainID = result.getString("domain");
		    		String rangeID = result.getString("range");
		    		
		    		argList.put(domainID,rangeID);
		    		
		    	}
		    	st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return argList;
	    	
	    }
	    
	    public static String getItemLabel(String itemId, String lang){
	        
	    	String label = null;
	    	PreparedStatement st;
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_LABEL);
				st.setString(1, itemId);
				st.setString(2, lang);
				ResultSet result = st.executeQuery();
		    	
		    	if(result.next()){
		    		
		    		label = result.getString("value");
		    	}
		    	result.close();
		    	st.close();
		    	
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return label;
	    	
	    }
	    
	    public static List<String> getItemAliases(String itemId, String lang){
	        
	    	List<String> aliases = new ArrayList<String>();
	    	PreparedStatement st;
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_ALIASES);
				st.setString(1, itemId);
				st.setString(2, lang);
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		
		    		String alias = result.getString("value");
		    		aliases.add(alias);
		    		
		    	}
		    	result.close();
		    	st.close();
		    	
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return aliases;
	    	
	    }
	    
	    
  public static List<String> getItemByLable(String label, String lang){
	        
	  	List<String>  itemList = new ArrayList<String>();
	    	PreparedStatement st;
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_BY_VALUE);
				st.setString(1, label);
				st.setString(2, lang);
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		
		    		label = result.getString("entity_id");
		    		
		    		itemList.add(label);
		    	}
		    	result.close();
		    	st.close();
		    	
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return itemList;
	    	
	    }
  
	    
	    public static String getItemDescription(String itemId, String lang){
	        
	    	String desc = null ;
	    	PreparedStatement st;
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_DESCRIPTION);
				st.setString(1, itemId);
				st.setString(2, lang);
				ResultSet result = st.executeQuery();
		    	
		    	if(result.next()){
		    		
		    		desc= result.getString("value");
		    	}
		    	result.close();
		    	st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return desc;
	    	
	    }
	    
	    public static List<String> getPropertyIdLis(){
	    	List<String> idList = new ArrayList<String>();
	    	PreparedStatement st;
	    	
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_PROPETY_IDS);
				
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		idList.add(result.getString("id"));
		    		
		    	}
		    	result.close();
		    	st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return idList;
	    	
	    }
	    
	    public static String getPropertyType(String propId){
	    	String type = null;
	    	PreparedStatement st;
	    	
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_PROPETY_RANGE_TYPE);
				st.setString(1, propId);
				ResultSet result = st.executeQuery();
		    	
		    	if(result.next()){
		    		
		    		type = result.getString("type");
		    		
		    	}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return type;
	    	
	    }
	    
	    
	    public static int getPropertyFrequency(String propId){
	    	int freq = -1;
	    	
	    	PreparedStatement st;
			
	    	try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_PROPETY_CLAIM_COUNT);
				st.setString(1, propId);
				
				ResultSet result = st.executeQuery();
		    	
		    	if(result.next()){
		    		
		    		freq=  result.getInt("total");
		    	}
		    	result.close();
		    	st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return freq;
	    	
	    }
	public static void main(String[] args) throws FileNotFoundException {
		
		collectPropertyStatistics("output/properties_statitics.txt");
//		PrintWriter out = new PrintWriter(new File("output/item_prop_overlap.csv"));
//		out.println("source property \t item with same label \t  label \t description \t aliases");
//		List<String> propIDList = getPropertyIdLis();
//		
//		for(String prop : propIDList){
//			
//			String attrLabel = getItemLabel(prop, "en");
//			System.out.println("Property: " + attrLabel);
//			System.out.println("Items with the same label");
//			
//			List<String> itemWithSameLabel = getItemByLable(attrLabel, "en");
//			
//			if(itemWithSameLabel.size() <= 1)
//				continue;
//			
//			for(String itemId : itemWithSameLabel ){
//				
//				if(itemId.equals(prop))
//					continue;
//				
//				System.out.println(itemId);
//				String itemLabel = getItemLabel(itemId, "en");
//				String itemDesc = getItemDescription(itemId, "en");
//				List<String > aliases = getItemAliases("Q1", "en");
//				String itemAliasesStr ="";
//				
//				if(aliases.size() > 0){
//					 itemAliasesStr =aliases.toString().replace("[", "").replace("]", "");
//				}
//
//
//				out.println(prop + "\t" + itemId + "\t" + itemLabel + "\t" + itemDesc + "\t" + itemAliasesStr);
//			}
//			
//			System.out.println("-------- \n");
//			out.println();
//			
//		}
//		out.close();
		
		
	}
	
	
	public static Map<String, Integer> collectPropertyStatistics(String outFile){
		PrintWriter out = null;
		try {
			out = new PrintWriter(new File(outFile));
			out.println("propId \t label \t description \t aliases \t  type \t count");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Map<String, Integer> propFreqMap = new HashMap<String, Integer>();
		List<String> propIdList = getPropertyIdLis();
		System.out.println("#Total properties: " + propIdList.size());
		
		for(String propId : propIdList){
			
			//Count the number of claims where this attribute has been used.
			int frq = getPropertyFrequency(propId);
		
			if(frq!=-1){
				propFreqMap.put(propId, frq);
				String label = getItemLabel(propId, "en");
				String desc = getItemDescription(propId, "en");
				String type = getPropertyType(propId);
				
				List<String > aliases = getItemAliases(propId, "en");
				String itemAliasesStr ="";
				
				if(aliases.size() > 0){
					 itemAliasesStr =aliases.toString().replace("[", "").replace("]", "");
				}


				out.println(propId + "\t" + label + "\t" + desc + "\t" + itemAliasesStr + "\t" + type + "\t" + frq);
				
				System.out.println(propId + "\t" + label + "\t" + desc + "\t" + itemAliasesStr + "\t" +type + "\t" + frq);
				out.flush();
			}
		}
		out.close();
		return propFreqMap;
		
	}
}



