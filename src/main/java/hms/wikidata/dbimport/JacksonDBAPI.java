package hms.wikidata.dbimport;

import hms.wikidata.model.WikiReference;

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
    
    private static final String SELECT_CLAIM_ARGUMENTS = "SELECT entity_id as domain, wikidata_item_value as \"range\" FROM wikidata_20150420.wiki_claims where claim_id =? limit ?" ;
   
    private static final String SELECT_ENTITY_STATEMENTS = "select claim_id from wiki_claims where entity_id = ?" ;
    
    private static final String SELECT_ENTITY_STATEMENTS_AND_VALUES = "select claim_id, simple_value, wikidata_item_value from wiki_claims where entity_id = ?" ;

    
    
    private static final String SELECT_ITEM_LABEL = "SELECT value FROM label where entity_id = ? and language =?";
   
    //Use like to apply case insensitive comparison
    private static final String SELECT_ITEM_BY_VALUE = "SELECT * FROM label where value like ? and language =?";

    
    private static final String SELECT_ITEM_DESCRIPTION = "SELECT value FROM description where entity_id = ? and language =?";
    
    
    private static final String SELECT_PROPETY_IDS = "SELECT id FROM item WHERE type = 'property' " ;
    
    private static final String SELECT_ITEM_IDS = "SELECT id FROM item WHERE type = 'item' " ;

    
    private static final String SELECT_PROPETY_CLAIM_COUNT = "SELECT count(*) as total FROM wikidata_20150420.wiki_claim where claim_id = ?";
    		
    private static final String SELECT_PROPETY_RANGE_TYPE = "SELECT type FROM wikidata_20150420.wiki_claim where claim_id = ? limit 1";

    private static final String SELECT_ITEM_ALIASES = "select * from alias where entity_id = ?  and language =?";

    private static final String SELECT_DISTINCT_ITEMS_PER_LANGUAGE_LABEL_BASED = "SELECT distinct(count(entity_id)) as nr FROM wikidata_20150420.label l , item i " +
    																 " where i.type ='item' and i.id = l.entity_id and l.language =? " ;

    
    private static final String SELECT_DISTINCT_ITEMS_HAVING_LABEL = "SELECT distinct entity_id as id, l.value as label FROM wikidata_20150420.label l , item i " +
    																  "where i.type ='item' and i.id = l.entity_id and l.language = ? limit 10000" ;

    
    
    private static final String SELECT_DISTINCT_ITEMS_PER_LANGUAGE_DESC_BASED = "SELECT distinct(count(entity_id)) as nr FROM description l , item i " +
			 " where i.type ='item' and i.id = l.entity_id and l.language =? " ;

    
    private static final String SELECT_WIKIDATA_LANGUAGES = "select distinct(language) as lang from label";
    
    
    private static final String SELECT_ITEM_CLAIM_REFERECEN_INFO = "SELECT c.entity_id,  c.claim_id, reference_prop, reference_value, reference_value_type " +
																	"FROM wikidata_20150420.wiki_claim_reference r, wiki_claim c " + 
																	"where c.entity_id = ? and  c.entity_id = r.entity_id and r.claim_id = c.claim_id " +
																	"and  reference_value_type =?" ;
    
    private static final String SELECT_REFERENCE_OCCURRENCE_COUNT = "select count(*) from wiki_claim_reference " +
    																"where reference_value= ? and reference_value_type = ?";
    
    
    private static final String SELECT_LABEL_COUNT = "select count(*) from label where language = ? and value like ?";
    
    
    private static final String SELECT_WIKIMEDIA_CATEGORIES = "select entity_id from wiki_claims where claim_id = 'P31' and wikidata_item_value = 'Q4167836' limit 100" ;
    


    /**
     * Get the IDs of the claims linked to a given entity, i.e., Q or P
     * @param entityId
     * @return
     */
    public static List<String> getEntityClaimsIds(String entityId){
    	
    	List<String> claimIdList= new ArrayList<String>();
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_ENTITY_STATEMENTS);
			st.setString(1, entityId);
			ResultSet result =  st.executeQuery();
			while(result.next()){
				claimIdList.add(result.getString(1));
				
			}
			
			st.close();
		}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return claimIdList;
    }
    
    /**
     * Get the IDs of the claims and the corresponding values for a given entity, i.e., Q or P
     * @param entityId
     * @return
     */
    public static Map<String,String> getEntityClaimsIdsAndValues(String entityId){
    	
    	Map<String,String> claimIdList= new HashMap<String,String>();
    	PreparedStatement st;

    	int ID = 0;
    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_ENTITY_STATEMENTS_AND_VALUES);
			st.setString(1, entityId);
			ResultSet result =  st.executeQuery();
			
			while(result.next()){
				String claimId = result.getString("claim_id");
				String simpleValue = result.getString("simple_value");
				String entityValue = result.getString("wikidata_item_value");
				
				if(simpleValue!= null){
					ID++;

					claimIdList.put(claimId+"#"+ID, simpleValue.replace("\"", "'"));
				}
				else{
					ID++;

					claimIdList.put(claimId+"#"+ID,entityValue);
				}
				
			}
			
			st.close();
		}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return claimIdList;
    }
 
    /**
     * Category pages are linked with instance of relation to Wikimedia category(Q4167836)
     */
    public static List<String> getWikimediaCategoryItemIds(){
    	List<String> categoriesIdList= new ArrayList<String>();
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_WIKIMEDIA_CATEGORIES);
			ResultSet result =  st.executeQuery();
			while(result.next()){
				categoriesIdList.add(result.getString(1));
				
			}
			
			st.close();
		}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return categoriesIdList;
			
    }
    public static int getLabelCount(String label, String lang){
    	
    	int count = 0;
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_LABEL_COUNT);
			st.setString(1, lang);
			st.setString(2, label);
			
			ResultSet result = st.executeQuery();
			
			if(result.next()){
				
				count = result.getInt(1);
				
			}
			st.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return count;
    }
 
    
    public static int getReferenceOccurrenceCount(String refId, String refType){
    	
    	int count = 0;
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_REFERENCE_OCCURRENCE_COUNT);
			st.setString(1, refId);
			st.setString(2, refType);
			
			ResultSet result = st.executeQuery();
			
			if(result.next()){
				
				count = result.getInt(1);
				
			}
			st.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return count;
    }
    public static List<WikiReference> getItemClaimReferenceInformation(String itemId, String reference_value_type){
    	
    	
    	List<WikiReference> itemClaimRefList = new ArrayList<WikiReference>();
    
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_ITEM_CLAIM_REFERECEN_INFO);
			st.setString(1, itemId);
			st.setString(2, reference_value_type);
			ResultSet result = st.executeQuery();
			
			while(result.next()){
				
				WikiReference wRef = new WikiReference();
				wRef.setSourceItemId(result.getString(1));
				wRef.setClaimId(result.getString(2));
				wRef.setReferenceId(result.getString(3));
				wRef.setReferenceValue(result.getString(4));
				wRef.setReferenceValueType(result.getString(5));
				
				itemClaimRefList.add(wRef);
				
				
			}
			st.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	return itemClaimRefList;
    }
    
    public static int getNrItemsLabelBased(String lang){

    	int count = 0 ;
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_DISTINCT_ITEMS_PER_LANGUAGE_LABEL_BASED);
			st.setString(1, lang);
			ResultSet result = st.executeQuery();
			
			if(result.next()){
				count = result.getInt("nr");
			}
			st.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return count;
	
    }
    
    public static int getNrItemsDescBased(String lang){

    	int count = 0 ;
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_DISTINCT_ITEMS_PER_LANGUAGE_DESC_BASED);
			st.setString(1, lang);
			ResultSet result = st.executeQuery();
			
			if(result.next()){
				count = result.getInt("nr");
			}
			st.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return count;
	
    }
    
    public static List<String> getWikidataLanguages(){

    	List<String> langList = new ArrayList<String>();
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_WIKIDATA_LANGUAGES);
			ResultSet result = st.executeQuery();
			
			while(result.next()){
				String lang = result.getString("lang") ;
				langList.add(lang);
				
			}
			st.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return langList;
	
    }

    
    
    public static String getClaimRange(String itemId, String calimId){

    	
    	return null;
    }
	  public static List<String> getClaimArguments(String calimId, String lang, int maxReturned){
	      
	    	List<String> argList = new ArrayList<String>();
	    	PreparedStatement st;
			try {
				st =WikidataToRDB.conn.prepareStatement(SELECT_CLAIM_ARGUMENTS);
				st.setString(1, calimId);
				st.setInt(2, maxReturned);
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		
		    		String domainID = result.getString("domain");
		    		String rangeID = result.getString("range");
		    		
		    		argList.add(domainID + "-" + rangeID);
		    		
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
	    
	    
  
	    public static Map<String,String> getItemLableMap(String lang){
	    
	    	
	    	Map<String,String> itemLabelMap = new HashMap<String, String>();
	    	
	    	
	    	PreparedStatement st;
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_DISTINCT_ITEMS_HAVING_LABEL);
				st.setString(1, lang);
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		
		    		String id = result.getString("id");
		    		String label = result.getString("label");
		    		itemLabelMap.put(id,label);
		    	}
		    	result.close();
		    	st.close();
		    	
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	    	
	    	return itemLabelMap;
	    	
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
	    
	    public static List<String> getItemIdLis(){
	    	List<String> idList = new ArrayList<String>();
	    	PreparedStatement st;
	    	
			try {
				st = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_IDS);
				
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		idList.add(result.getString("id"));
		    		
		    	}
		    	result.close();
		    	st.close();
			} catch (SQLException e) {
				
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
		
		
		Map<String, String> val = getEntityClaimsIdsAndValues("Q5");
		for(String key:val.keySet()){
			System.out.println( getItemLabel(key.split("#")[0], "en") + "\t" + getItemLabel(val.get(key), "en"));
		}
//		List<String> catIds = getWikimediaCategoryItemIds();
//		
//		for(String catId : catIds){
//			
//			System.out.println(catId + "\t" + getItemLabel(catId, "en"));
//			
//			
//		}
//		
//		String propId = "P69"; 
//		String propLabel = getItemLabel(propId, "en");
//	
//		List<String> res = getClaimArguments(propId, "en", 10);
//		for(String arg : res){
//			String argList[] = arg.split("-");
//			System.out.println(getItemLabel(argList[0],"en") + " " + propLabel + " " + getItemLabel(argList[1],"en"));
//			
//		}
//		
//		String itemId = "Q76";
//		String lang = "en";
//		String refType = "wikibase-item";
////		String refType = "string";
//		List<WikiReference> rList = getItemClaimReferenceInformation(itemId,refType);
//		for(WikiReference r: rList){
//			System.out.println("Item: " + getItemLabel(itemId, lang));
//			System.out.println("Claim: " + getItemLabel(r.getClaimId(), lang));
//			System.out.println("Reference Property: " + getItemLabel(r.getReferenceId(), lang));
//			if(refType.equals("wikibase-item")){
//				System.out.println("Reference Value: " + getItemLabel(r.getReferenceValue(), lang));
//			}
//			else{
//				System.out.println("Reference Value: " + r.getReferenceValue());
//			}
//			System.out.println("Reference Value Type: " + r.getReferenceValueType());
//			System.out.println();
//		}
		
	}

	
	/**
	 * Get the number of wikidata items per language
	 * @param labelBased Items will be counted based on the existence of a corresponding label, otherwise, descriptions will be considered
	 * @param csvOutputFile The path of the file where the results are stored
	 */
	public static void getNumberOfItems(boolean labelBased, String csvOutputFile) {
		
		try {
			PrintWriter out = new PrintWriter(new File(csvOutputFile));
			
			if(labelBased){
				out.println("Language, #Items (label-based)" );
			}
			else{
				out.println("Language, #Items (description_based)" );
			}
			
			List<String> langList = getWikidataLanguages();
						
			for(String lang: langList){
				int nrItems = 0 ;
				
				if(labelBased){
					nrItems = getNrItemsDescBased(lang);
				}
				
				else{
					nrItems = getNrItemsLabelBased(lang);
				}
				
				System.out.println(lang + "\t" + nrItems);
				
				out.println(lang + "\t" + nrItems);
			}
			
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	public static Map<String, Integer> collectPropertyStatistics(String outFile){
		PrintWriter out = null;
		try {
			out = new PrintWriter(new File(outFile,"UTF-8"));
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



