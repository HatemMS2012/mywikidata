package hms.wikidata.dbimport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

public class WikidataToRDB
{

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/wikidata_20150420";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "admin";

    static final String insert_entity = "INSERT INTO entity (id,lang) VALUES (?,?)";
    static final String insert_label = "INSERT INTO label (entity_id,language,value) VALUES (?, ?, ?)";
    static final String insert_description = "INSERT INTO description (entity_id,language,value) VALUES (?, ?, ?)";
    static final String insert_alias = "INSERT INTO alias (entity_id,language,value) VALUES (?, ?, ?)";
    static final String insert_claims = "INSERT INTO claim(claim_id,entity_id,language,value,type)"+
                                        "VALUES (?,?,?,?,?);";

    static final String check_entity_exist = "SELECT id FROM entity WHERE id = ? and lang = ?";

    static final String check_relation_exist = "SELECT entity_id FROM claim WHERE entity_id =? and claim_id = ?  and language = ?";



    static Connection conn = null;
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

    private static void insertAliases(String itemID, String lang, List<String> entity_aliases)
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

    private static void insertDescription(String itemID, String lang, String entity_desc)
        throws SQLException
    {
        PreparedStatement insertDescSt = conn.prepareStatement(insert_description);
        insertDescSt.setString(1, itemID);
        insertDescSt.setString(2, lang);
        insertDescSt.setString(3, entity_desc);

        insertDescSt.execute();
    }

    private static void insertLabel(String itemID, String lang, String entity_lable)
        throws SQLException
    {
        PreparedStatement insertLabelSt = conn.prepareStatement(insert_label);
        insertLabelSt.setString(1, itemID);
        insertLabelSt.setString(2, lang);
        insertLabelSt.setString(3, entity_lable);
        insertLabelSt.execute();
    }

    private static void insertEntity(String itemID, String lang)
        throws SQLException
    {
        PreparedStatement insertEntitySt = conn.prepareStatement(insert_entity);
        insertEntitySt.setString(1, itemID);
        insertEntitySt.setString(2, lang);
        insertEntitySt.execute();
    }

    public static void main(String[] args) throws SQLException
    {

        String itemId = "Q5891";
        insert(itemId,"en");
        insert(itemId,"de");
        insert(itemId,"fr");
        insert(itemId,"es");
        insert(itemId,"it");
        insert(itemId,"ar");
    }

}
