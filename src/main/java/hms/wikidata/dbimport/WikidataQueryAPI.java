package hms.wikidata.dbimport;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WikidataQueryAPI
{


    private static final String SELECT_ITEM_RELATION_INFO = "SELECT l.value as relation, c.value value, c.language as lang, c.type as rel_type " +
                                                    "FROM claim c, label l " +
                                                    "where c.claim_id = l.entity_id " +
                                                    "and c.language = l.language "   +
                                                    "and c.entity_id = ? " +
                                                    "and c.type =? " +
                                                    "and c.language = ? " ;
    private static final String SELECT_ITEM_RELATION_INFO_NO_TYPE = "SELECT l.value as relation, c.value value, c.language as lang, c.type as rel_type " +
            "FROM claim c, label l " +
            "where c.claim_id = l.entity_id " +
            "and c.language = l.language "   +
            "and c.entity_id = ? " +
            "and c.language = ? " ;

    private static final String SELECT_ITEM_LABLE = "SELECT value from label WHERE entity_id = ? and language = ? ";
    private static final String SELECT_ITEM_DESCRIPTION = "SELECT value from description WHERE entity_id = ? and language = ? ";

    private static final String SELECT_ITEM_ALIASES = "SELECT value from alias WHERE entity_id = ? and language = ? ";
    private static final String SELECT_ITEM_BY_LABEL_SEARCH = "SELECT entity_id, value from label WHERE lower(value) like ? and language = ?" ;

    public static Map<String, String> getItemRelationRealizations(String itemID, String relationType, String lang) throws SQLException{


        Map<String, String> relationMap = new HashMap<String, String>();

        PreparedStatement selectRelSt = null;
        if(relationType == null){
            selectRelSt = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_RELATION_INFO_NO_TYPE);
            selectRelSt.setString(1, itemID);
            selectRelSt.setString(2, lang);


        }
        else{
            selectRelSt = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_RELATION_INFO);

            selectRelSt.setString(1, itemID);
            selectRelSt.setString(2, relationType);
            selectRelSt.setString(3, lang);

        }
        ResultSet res = selectRelSt.executeQuery();
        while(res.next()) {

            String relationName = res.getString("relation");
            String relationValue = res.getString("value");
            relationMap.put(relationName, relationValue);
            //System.out.println(relationName + "\t" + relationValue + "\t" + relationType + "\t" + lang);
        }

        return relationMap;

    }

    public static String getItemLabel(String itemID, String lang) throws SQLException{

        PreparedStatement selectLabelSt = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_LABLE);
        selectLabelSt.setString(1, itemID);
        selectLabelSt.setString(2, lang);
        ResultSet res = selectLabelSt.executeQuery();
        if(res.next()) {

            String itemLabel = res.getString("value");

            return itemLabel;
        }

        return null;

    }

    public static String getItemDescription(String itemID, String lang) throws SQLException{

        PreparedStatement selectDescSt = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_DESCRIPTION);
        selectDescSt.setString(1, itemID);
        selectDescSt.setString(2, lang);
        ResultSet res = selectDescSt.executeQuery();
        if(res.next()) {

            String itemDesc = res.getString("value");

            return itemDesc;
        }

        return null;

    }
    public static List<String> getItemAliases(String itemID, String lang) throws SQLException{

        List<String> aliasList = new ArrayList<String>();

        PreparedStatement selectAliasSt = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_ALIASES);
        selectAliasSt.setString(1, itemID);
        selectAliasSt.setString(2, lang);
        ResultSet res = selectAliasSt.executeQuery();
        while(res.next()) {

            String itemAlias = res.getString("value");
            aliasList.add(itemAlias);

        }

        return aliasList;

    }

    public static List<String> getItemBySearch(String keyword, String lang) throws SQLException{

        List<String> searchResults = new ArrayList<String>();

        PreparedStatement selectLabelSt = WikidataToRDB.conn.prepareStatement(SELECT_ITEM_BY_LABEL_SEARCH);
        selectLabelSt.setString(1, "%"+keyword.toLowerCase()+"%");
        selectLabelSt.setString(2, lang);
        ResultSet res = selectLabelSt.executeQuery();
        while(res.next()) {

//            String itemLabel = res.getString("value");
            String itemID = res.getString("entity_id");

            searchResults.add(itemID);

        }

        return searchResults;

    }



    public static void main(String[] args) throws SQLException
    {

        example0();
        example1();

    }

    public static void example0() throws SQLException{

          String itemId = "Q2";
          String lang ="en";

          System.out.println("Wikidata Entry: "+ getItemLabel(itemId, lang));
          System.out.println("Description: "+ getItemDescription(itemId, lang));
          System.out.println("Aliases: "+ getItemAliases(itemId, lang));
          System.out.println("------------- Relations -----------");
          System.out.println(getItemRelationRealizations(itemId, "wikibase-entityid", lang));

          System.out.println();
    }


    public static void example1() throws SQLException{
        String keyword = "city";
        String lang = "en";
        //wikibase-entityid, string, time
        System.out.println("Search resutls for " + keyword + " in " + lang);

        System.out.println("Search results.....");
        List<String> searchResults = getItemBySearch(keyword, lang);

        int i = 1 ;
        for(String searchResult : searchResults){
            System.out.println(i+ "\tItem ID: " + searchResult);
            System.out.println("\tWikidata Entry: "+ getItemLabel(searchResult, lang));
            System.out.println("\tDescription: "+ getItemDescription(searchResult, lang));
            System.out.println("\tAliases: "+ getItemAliases(searchResult, lang));
            System.out.println("\tRelations: "+ getItemRelationRealizations(searchResult, null, lang));
            System.out.println();
            i++;

        }
    }

}
