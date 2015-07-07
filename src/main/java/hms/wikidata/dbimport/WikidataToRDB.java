package hms.wikidata.dbimport;

import hms.wikidata.model.ClaimItem;
import hms.wikidata.old.WikiDataQueryTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WikidataToRDB
{

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/wikidata_20150420";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "admin";

    static final String insert_entity = "INSERT INTO entity (id,lang) VALUES (?,?)";
    static final String insert_label = "INSERT INTO label (entity_id,language,value) VALUES (?, ?, ?)";
    
    static final String insert_item = "INSERT INTO item (id,type) VALUES (?,?)";

    static final String insert_description = "INSERT INTO description (entity_id,language,value) VALUES (?, ?, ?)";
    static final String insert_alias = "INSERT INTO alias (entity_id,language,value) VALUES (?, ?, ?)";
    static final String insert_claims = "INSERT INTO claim(claim_id,entity_id,language,value,type)"+
                                        "VALUES (?,?,?,?,?);";

    
    static final String insert_claim_wiki_entity_type = "INSERT INTO wiki_claims(entity_id,claim_id,simple_value,wikidata_item_value,type) VALUES (?,?,null,?,?);";
    static final String insert_claim_simple_type = "INSERT INTO wiki_claims(entity_id,claim_id,simple_value,wikidata_item_value,type) VALUES (?,?,?,null,?);";
    
    
    static final String insert_claim_reference = " INSERT INTO item_claim_reference (entity_id,    claim_id,  calim_target_id,  reference_prop,   "
    																										+ " reference_value_type,    reference_value, reference_hash)"
    																										+ "    VALUES (?,?,?,?,?,?,?)";
 
    
    static final String check_entity_exist = "SELECT id FROM entity WHERE id = ? and lang = ?";
    static final String check_claim_exist = "SELECT claim_id FROM wiki_claim WHERE claim_id = ? and entity_id = ?";

    
    static final String check_label_exist = "SELECT entity_id FROM label WHERE entity_id = ? and language = ?";
    static final String check_label_entity_exist = "SELECT entity_id FROM label WHERE entity_id = ?";


    static final String check_relation_exist = "SELECT entity_id FROM claim WHERE entity_id =? and claim_id = ?  and language = ?";
    static final String select_distinct_label_entity_ids= "select distinct entity_id from label" ;
    static final String select_distinct_alias_entity_ids= "select distinct entity_id from alias" ;
    static final String select_distinct_claims= "select entity_id,claim_id from alias" ;


    static final String select_distinct_desc_entity_ids= "select distinct entity_id from description" ;


    //Queries
    
    private static final String SELECT_CLAIM_ARGUMENTS = "SELECT entity_id as domain, wikidata_item_value as \"range\" FROM wikidata_20150420.wiki_claim where claim_id =?" ;
    private static final String SELECT_ITEM_LABEL = "SELECT value FROM label where entity_id = ? and language =?";
    private static final String SELECT_LABELS = "SELECT value FROM label where language =? limit 10";

    
    
    public static Connection conn = null;
    Statement stmt = null;
    static{
        try{
           //STEP 2: Register JDBC driver
           Class.forName("com.mysql.jdbc.Driver");

           //STEP 3: Open a connection
//           System.out.println("Connecting to database...");
           conn = DriverManager.getConnection(DB_URL,USER,PASS);
        }
        catch(SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void init(){
    	
    	try {
    		//STEP 2: Register JDBC driver
	        Class.forName("com.mysql.jdbc.Driver");
	
	        //STEP 3: Open a connection
	//        System.out.println("Connecting to database...");
	        conn = DriverManager.getConnection(DB_URL,USER,PASS);
	     }
	     catch(SQLException e){
	         e.printStackTrace();
	     }
	     catch (ClassNotFoundException e) {
	         // TODO Auto-generated catch block
	         e.printStackTrace();
	     }
    }
    
    public static Map<String, String> getClaimArguments(String calimId, String lang){
      
    	Map<String, String> argList = new HashMap<String, String>();
    	PreparedStatement st;
		try {
			st = conn.prepareStatement(SELECT_CLAIM_ARGUMENTS);
			st.setString(1, calimId);
			ResultSet result = st.executeQuery();
	    	
	    	while(result.next()){
	    		
	    		String domainID = result.getString("domain");
	    		String rangeID = result.getString("range");
	    		
	    		
	    		argList.put(getItemLabel(domainID, lang),getItemLabel(rangeID, lang));
	    	}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	return argList;
    	
    }
    
    public static String getItemLabel(String itemId, String lang){
        
    	String label = null ;
    	PreparedStatement st;
		try {
			st = conn.prepareStatement(SELECT_ITEM_LABEL);
			st.setString(1, itemId);
			st.setString(2, lang);
			ResultSet result = st.executeQuery();
	    	
	    	if(result.next()){
	    		
	    		label = result.getString("value");
	    		st.close();
	    		result.close();
	    	}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	return label;
    	
    }
    
	public static List<String> getLabels(String lang){
		
		List<String> labels = new ArrayList<String>();
		
		PreparedStatement st;
			try {
				st = conn.prepareStatement(SELECT_LABELS);
				st.setString(1, lang);
				ResultSet result = st.executeQuery();
		    	
		    	while(result.next()){
		    		
		    		String label = result.getString("value");
		    		labels.add(label);
		    	}
		    	st.close();
		    	result.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	return labels;
	    	
	    }
    
    
    public static Set<String> getEntitiesWithLabels(){
    	
    	Set<String> ids = new HashSet<String>();
    	
    	Statement st;
		try {
			st = conn.createStatement();
			ResultSet result = st.executeQuery(select_distinct_label_entity_ids);
			
			while(result.next()){
				ids.add(result.getString("entity_id"));
			}
			result.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    	return ids;
    	
    }
    
    public static Set<String> getEntitiesWithAliases(){
    	
    	Set<String> ids = new HashSet<String>();
    	
    	Statement st;
		try {
			st = conn.createStatement();
			ResultSet result = st.executeQuery(select_distinct_alias_entity_ids);
			
			while(result.next()){
				ids.add(result.getString("entity_id"));
			}
			result.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    	return ids;
    	
    }
//    public static Set<String> getInsertedClaims(){
//    	
//    	Set<String> ids = new HashSet<String>();
//    	
//    	Statement st;
//		try {
//			st = conn.createStatement();
//			ResultSet result = st.executeQuery(select_distinct_claims);
//			
//			while(result.next()){
//				ids.add(result.getString("entity_id"));
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	
//    	
//    	
//    	return ids;
//    	
//    }
    
    public static Set<String> getEntitiesWithDescription(){
    	
    	Set<String> ids = new HashSet<String>();
    	
    	Statement st;
		try {
			st = conn.createStatement();
			ResultSet result = st.executeQuery(select_distinct_desc_entity_ids);
			
			while(result.next()){
				ids.add(result.getString("entity_id"));
			}
			result.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    	return ids;
    	
    }
    
    

    public static void insert(String itemID, String lang) throws SQLException{

        if(!isEntityAlreadyInDB(itemID,lang)) {
            String entity_lable =  WikiDataQueryTool.getItemLabels(itemID, lang);
            String entity_desc =  WikiDataQueryTool.getItemDescriptions(itemID, lang);
            List<String> entity_aliases = WikiDataQueryTool.getItemAliases(itemID, lang);
            List<ClaimItem> entity_props = WikiDataQueryTool.getItemProperties(itemID, lang);

            System.out.println("[" + new Date() + "] Inserting: " + itemID + "@" + lang);

            insertEntity(itemID, lang);
            if(entity_lable !=null){
                insertLabel(itemID, lang, entity_lable);
            }
            if(entity_desc != null){
                insertDescription(itemID, lang, entity_desc);
            }
            if(entity_aliases !=null){
                insertAliases(itemID, lang, entity_aliases);
            }
            if(entity_props !=null){
                insertClaims(itemID, lang, entity_props);
            }
        }
        else{
//            System.out.println("Item is already in the database: " + itemID + "@" + lang);
        }



    }

    public static boolean isEntityAlreadyInDB(String itemID, String lang) throws SQLException{

        PreparedStatement checkSt = conn.prepareStatement(check_entity_exist);
        checkSt.setString(1, itemID);
        checkSt.setString(2, lang);
        ResultSet res = checkSt.executeQuery();
        if(res.next()) {
            return true;
        }
        return false;


    }
    
    public static boolean isLabelAlreadyInDB(String itemID, String lang) throws SQLException{

        PreparedStatement checkSt = conn.prepareStatement(check_label_exist);
        checkSt.setString(1, itemID);
        checkSt.setString(2, lang);
        ResultSet res = checkSt.executeQuery();
        if(res.next()) {
            return true;
        }
        return false;


    }
    
    public static boolean isLabelEntityAlreadyInDB(String itemID) throws SQLException{

        PreparedStatement checkSt = conn.prepareStatement(check_label_entity_exist);
        checkSt.setString(1, itemID);
        
        ResultSet res = checkSt.executeQuery();
        if(res.next()) {
        	checkSt.close();
            return true;
        }
    	checkSt.close();
        return false;


    }

    public static boolean isRelationAlreadyInDB(String itemID, String claimID, String lang) throws SQLException{

        PreparedStatement checkSt = conn.prepareStatement(check_relation_exist);
        checkSt.setString(1, itemID);
        checkSt.setString(2, claimID);
        checkSt.setString(3, lang);
        ResultSet res = checkSt.executeQuery();
        if(res.next()) {
            return true;
        }
        return false;


    }
    private static void insertClaims(String itemID, String lang, List<ClaimItem> entity_props)
        throws SQLException
    {
        for(ClaimItem propId : entity_props){


           // if(!isRelationAlreadyInDB(itemID, propId.getPropertyID(), lang)){
                PreparedStatement insertPropSt = conn.prepareStatement(insert_claims);
                insertPropSt.setString(1, propId.getPropertyID());
                insertPropSt.setString(2, itemID);
                insertPropSt.setString(3, lang);
                insertPropSt.setString(4, propId.getPropertyValue());
                insertPropSt.setString(5, propId.getPropertyDataType());
                insertPropSt.execute();
                insert(propId.getPropertyID(), lang);
            //}

        }
    }
    
    public static void insertClaim(String itemID,  String property, String propValue, String propType)
            throws SQLException {
            
    	PreparedStatement insertPropSt = null;

    	if(propType.equals("wikibase-item")){
    		insertPropSt = conn.prepareStatement(insert_claim_wiki_entity_type);
    	}
    	else{
    		insertPropSt = conn.prepareStatement(insert_claim_simple_type);
    	}
        
   
        insertPropSt.setString(1, itemID);
     	insertPropSt.setString(2, property);
    	insertPropSt.setString(3, propValue);
    	insertPropSt.setString(4, propType);
    	insertPropSt.execute();
    	insertPropSt.close();
        
    }
    
    
    public static void insertClaimReference(String entity_id, String    claim_id, String targetOfClaim,  String   reference_prop,
    		String  reference_value_type, String    reference_value, String  reference_hash)
            throws SQLException {
            
    	PreparedStatement insertPropSt = null;

    	
    	insertPropSt = conn.prepareStatement(insert_claim_reference);
    	
   
        insertPropSt.setString(1, entity_id);
     	insertPropSt.setString(2, claim_id);
     	insertPropSt.setString(3, targetOfClaim);
    	insertPropSt.setString(4, reference_prop);
    	insertPropSt.setString(5, reference_value_type);
    	insertPropSt.setString(6, reference_value);
    	insertPropSt.setString(7, reference_hash);
    	insertPropSt.execute();
    	insertPropSt.close();
        
    }

    public static void insertAliases(String itemID, String lang, List<String> entity_aliases)
        throws SQLException
    {
        for(String entityAlias : entity_aliases){


            PreparedStatement insertAliasSt = conn.prepareStatement(insert_alias);
            insertAliasSt.setString(1, itemID);
            insertAliasSt.setString(2, lang);
            insertAliasSt.setString(3, entityAlias);
                insertAliasSt.execute();

        }
    }
    
   
    public static void insertAlias(String itemID, String lang, String alias)
            throws SQLException{
    

                PreparedStatement insertAliasSt = conn.prepareStatement(insert_alias);
                insertAliasSt.setString(1, itemID);
                insertAliasSt.setString(2, lang);
                insertAliasSt.setString(3, alias);
                insertAliasSt.execute();
                insertAliasSt.close();
    
        }

    public static void insertDescription(String itemID, String lang, String entity_desc)
        throws SQLException
    {
        PreparedStatement insertDescSt = conn.prepareStatement(insert_description);
        insertDescSt.setString(1, itemID);
        insertDescSt.setString(2, lang);
        insertDescSt.setString(3, entity_desc);

        insertDescSt.execute();
        insertDescSt.close();
    }
    
    

    public static void insertLabel(String itemID, String lang, String entity_lable) throws SQLException{
       
    
        PreparedStatement insertLabelSt;
		
			insertLabelSt = conn.prepareStatement(insert_label);
			insertLabelSt.setString(1, itemID);
	        insertLabelSt.setString(2, lang);
	        insertLabelSt.setString(3, entity_lable);
	        insertLabelSt.execute();
	        insertLabelSt.close();
	    
	
    }
        

    private static void insertEntity(String itemID, String lang)
        throws SQLException
    {
        PreparedStatement insertEntitySt = conn.prepareStatement(insert_entity);
        insertEntitySt.setString(1, itemID);
        insertEntitySt.setString(2, lang);
        insertEntitySt.execute();
    }
    public static void insertItem(String itemID, String type)
            throws SQLException
        {
            PreparedStatement insertEntitySt = conn.prepareStatement(insert_item);
            insertEntitySt.setString(1, itemID);
            
            insertEntitySt.setString(2, type);
            insertEntitySt.execute();
            insertEntitySt.close();
        }

    public static void main(String[] args) throws SQLException
    {

        System.out.println(getLabels("en"));
    }

}
