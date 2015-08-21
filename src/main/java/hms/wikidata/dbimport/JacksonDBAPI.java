package hms.wikidata.dbimport;


import hms.wikidata.model.ClaimRealization;
import hms.wikidata.model.ExperimentalArgTypes;
import hms.wikidata.model.PropertyOfficialCategory;
import hms.wikidata.model.StructuralPropertyMapper;
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
import java.util.Random;

public class JacksonDBAPI {

	
  //Queries
    
    private static final String SELECT_CLAIM_ARGUMENTS = "SELECT entity_id as domain, wikidata_item_value as \"range\", simple_value as range2 FROM wikidata_20150420.wiki_claims where claim_id =? limit ?" ;

    private static final String SELECT_CLAIM_RANGE = "SELECT entity_id as domain, wikidata_item_value as \"range\", simple_value as range2 FROM wikidata_20150420.wiki_claims where entity_id = ? and claim_id =?" ;

    private static final String SELECT_CLAIM_ARGUMENTS_RANDOM = "SELECT entity_id as domain, wikidata_item_value as \"range\" FROM wikidata_20150420.wiki_claims where claim_id =?   limit 100" ;
    private static final String SELECT_CLAIM_REALIZATION = "SELECT entity_id as domain, wikidata_item_value as \"range\" FROM wikidata_20150420.wiki_claims where claim_id =? limit ?" ;

    
    private static final String SELECT_CLAIM_ARGUMENTS_RANDOM_TOTAL = "select count(*) from (SELECT * FROM wikidata_20150420.wiki_claims where claim_id =? limit 1000) ff";
    
    
  
 
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
    
    
    private static final String SELECT_OFFICIAL_CATEGORIZED_PROPERTIES = "SELECT * FROM wikidata_20150420.wikidata_prop_official_category where category = ?";
    
    
    private static final String SELECT_EXPRIMENTAL_PROPERTY_ARG_TYPES = "SELECT t1.id, t1.arg1, t1.arg2, t2.arg1 as real_arg1, t2.arg2 as real_arg2 "
    																  + "FROM arg_type_expriment_reduced t1, arg_type_from_realizations_expriment t2 "
    																  + "where t1.id = t2.id  and t1.id = ?";
    

    
    
    private static final String SELECT_EXPRIMENTAL_FN_ALIGNMENT_FOR_PROPERTY = "SELECT * FROM wikidata_20150420.fn_wikidata where sim_method = ? and propId =? order by rank asc";

    
    private static final String SELECT_ENTITY_ID = "select * from item where type =?";
    
    /**
     * Get the argument types of a given property.
     * NOTE: the type extraction procedure is still experimental
     * @param propId
     * @return
     */
    public static ExperimentalArgTypes getExperimentalArgTypes(String propId){
    
    	ExperimentalArgTypes argTypes = new ExperimentalArgTypes();
    	
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_EXPRIMENTAL_PROPERTY_ARG_TYPES);
			st.setString(1,propId);
		
			ResultSet result =  st.executeQuery();
		
			if(result.next()){
				
				argTypes.setId(propId);
				argTypes.setLabel(getItemLabel(propId, "en"));
				argTypes.setDescription(getItemDescription(propId,"en"));
				
				String arg1 = result.getString("arg1").replace("{", "").replace("}", "");
				String arg2 = result.getString("arg2").replace("{", "").replace("}", "");
				
				String realArg1 = ", REAL=" + result.getString("real_arg1");
				String realArg2 = ", REAL=" + result.getString("real_arg2");
				
				argTypes.setTypeArg1("{"+arg1+realArg1+"}");
				argTypes.setTypeArg2("{"+arg2+realArg2+"}");
			}
			
			st.close();
		}
	    catch (SQLException e) {
			
			e.printStackTrace();
		}

    	return argTypes;
    }

    
    /**
     * Get experimental matching frame from a given property
     * @param propId
     * @return
     */
    public static List<String> getExperimentalFNAlignment(String propId,String method){
        
    	
    	PreparedStatement st;
    	List<String> matchingFrames = new ArrayList<String>();

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_EXPRIMENTAL_FN_ALIGNMENT_FOR_PROPERTY);
			st.setString(2,propId);
			st.setString(1,method);
			
			ResultSet result =  st.executeQuery();
		
			while(result.next()){
				
				String frameId = result.getString("frameId");
				String arg1 = result.getString("arg1_role");
				String arg2 = result.getString("arg2_role");
				
				String r = frameId+":"+arg1+"#"+arg2;
				matchingFrames.add(r);
			}
			st.close();
    	}
    	
    	 catch (SQLException e) {
 			
 			e.printStackTrace();
 		}
    	return matchingFrames;
	}
    
    /**
     * Get a list of entities of a given type, i.e., property or item
     * @param type
     * @return
     */
    public static List<String> getEntityIdList(String type){
        
    	
    	PreparedStatement st;
    	List<String> entityIdList = new ArrayList<String>();

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_ENTITY_ID);
			st.setString(1,type);
			
			
			ResultSet result =  st.executeQuery();
		
			while(result.next()){
				
				entityIdList.add(result.getString("id"));
			}
			st.close();
    	}
    	
    	 catch (SQLException e) {
 			
 			e.printStackTrace();
 		}
    	return entityIdList;
	}
    /**
     * Get the list of ids for the properties that were categorized by Wikidata people
     * @return
     */
    public static List<String> getOfficialProperties(PropertyOfficialCategory category){
    	
    	List<String> propIdList= new ArrayList<String>();
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_OFFICIAL_CATEGORIZED_PROPERTIES);
			st.setString(1, category.toString());
			ResultSet result =  st.executeQuery();
			while(result.next()){
				propIdList.add(result.getString("prop_id"));
				
			}
			
			st.close();
		}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return propIdList;
    }
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

    
    
    public static List<String> getClaimRange(String itemId, String calimId){
    
    	List<String> propRange = new ArrayList<String>();
    	PreparedStatement st;

    	try {
			st =WikidataToRDB.conn.prepareStatement(SELECT_CLAIM_RANGE);
			st.setString(1, itemId);
			st.setString(2, calimId);
			
			ResultSet result = st.executeQuery();
			
			while(result.next()){
			
				String range1 = result.getString("range");
				String range2 = result.getString("range2");
				
				if(range1 != null){
					propRange.add(range1);
				}
				else if(range2 != null){
					propRange.add(range2);
				}
				
				
			}
			st.close();
			result.close();
    	}
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return propRange;
	    
    	

    }
    
    /**
     * Get a list of items that participate in a given statement
     * @param calimId
     * @param lang
     * @param maxReturned
     * @return
     */
	  public static List<String> getClaimArguments(String calimId, int maxReturned){
	      
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
		    		String rangeIDSimpleValue = result.getString("range2");
		    		
		    		if(rangeID!= null){
		    		
		    			argList.add(domainID + "-" + rangeID);
		    		}
		    		else if(rangeIDSimpleValue !=null){
		    			argList.add(domainID + "-" + rangeIDSimpleValue);

		    		}
		    		
		    	}
		    	st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return argList;
	    	
	    }
	  
	  public static int getClaimArgumentsCountForRandom(String calimId, String lang){
		  
		  int total = 0;
		  
		  PreparedStatement st;
			try {
				st =WikidataToRDB.conn.prepareStatement(SELECT_CLAIM_ARGUMENTS_RANDOM_TOTAL);
				st.setString(1, calimId);
				ResultSet result = st.executeQuery();
				if(result.next()){
					
					total = result.getInt(1);
					
				}
				result.close();
				st.close();
				
		  } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return total;
	  }
	  
	  /**
	   * Get a relation of specific property
	   * @param calimId
	   * @param lang
	   * @return
	   */
	  public static String getClaimArgumentsRandom(String calimId, String lang){
	      
		    Random randomGenerator = new Random();

		   	String arguments = null;
	    	
	    	PreparedStatement st;
			try {
				st =WikidataToRDB.conn.prepareStatement(SELECT_CLAIM_ARGUMENTS_RANDOM);
				st.setString(1, calimId);
				
				int i = 1 ;
			
				int total = getClaimArgumentsCountForRandom(calimId, lang);
				
				int randomInt = 1+ (randomGenerator.nextInt(total));
				
			
				
				ResultSet result = st.executeQuery();
		    	
		    	while(i!=randomInt){
		    		result.next();
		    		i ++ ;
		    	}
		    	if(result.next()){	
			    	String domainID = result.getString("domain");
			    	String rangeID = result.getString("range");
			    		
			    	arguments = domainID + "-" + rangeID;
		    	}
		    	
		    	
		    	
		    	result.close();
		    	st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return arguments;
	    	
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
	    
	    /**
	     * Get all realization of a specific property
	     * @param propId
	     * @param max
	     * @return
	     */
	public static List<ClaimRealization> getClaimRelatization(String propId, int max) {

		List<ClaimRealization>  realizationList = new ArrayList<ClaimRealization>();

		PreparedStatement st;
		try {
			st = WikidataToRDB.conn.prepareStatement(SELECT_CLAIM_REALIZATION);
			st.setString(1, propId);
			st.setInt(2, max);

			ResultSet result = st.executeQuery();

			while (result.next()) {
				String domainID = result.getString("domain");
				String rangeID = result.getString("range");
				ClaimRealization cRealization = new ClaimRealization();
				cRealization.setPropId(propId);
				cRealization.setDomain(domainID);
				cRealization.setRange(rangeID);
				
				realizationList.add(cRealization);
			}

			result.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return realizationList;

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
		System.out.println(getExperimentalArgTypes("P1000"));

//		System.out.println(getExperimentalFNAlignment("P22","WN"));
//		List<ClaimRealization> r = getClaimRelatization("P108",1000);
//		System.out.println(r.size());
//		System.out.println(getClaimArgumentsRandom("P1534", "en"));
		
//		for (PropertyOfficialCategory type : PropertyOfficialCategory.values()) {
//
//			System.out.println(type);
//			System.out.println(".................");
//			List<String> propList = getOfficialProperties(type);
//			for(String propId : propList){
//				String propType = StructuralPropertyMapper.structuarlPropertiesMap.get("instanceOf");
//				String res = getClaimRange(propId, propType);
//				
//				if(res!=null){
//					System.out.println(getItemLabel(propId,"en") + " " + getItemLabel(propType, "en") + " " + getItemLabel(res,"en"));
//				}
//	
//			}
//			System.out.println(" ------------- ");
//		}
//		
		
//		Map<String, String> val = getEntityClaimsIdsAndValues("Q5");
//		for(String key:val.keySet()){
//			System.out.println( getItemLabel(key.split("#")[0], "en") + "\t" + getItemLabel(val.get(key), "en"));
//		}
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



