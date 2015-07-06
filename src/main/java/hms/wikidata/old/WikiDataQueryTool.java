package hms.wikidata.old;

import hms.wikidata.model.ClaimItem;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import com.rethinkdb.ast.query.gen.Get;
import com.rethinkdb.ast.query.gen.GetField;

public class WikiDataQueryTool
{

    private static String db_name = "wikidata";
    private static String main_table = "data";
    private static RethinkDB r;
    private static RethinkDBConnection con;
    static {

        r = RethinkDB.r;
        con = r.connect("130.83.167.161",28015);

        con.use(db_name);
    }



    /**
     * Query wikidata using item id
     * @param itemID Item ID
     * @param lang Language
     */
    public static void queryWikiData(String itemID, String lang){


        System.out.println("Wikidata Entry : " +  getItemLabels(itemID, lang));
        System.out.println("Descriptions : " +  getItemDescriptions(itemID, lang));
        System.out.println("Aliases : " +  getItemAliases(itemID, lang));
        System.out.println();

        System.out.println("Item Poperties");

        List<ClaimItem> props = getItemProperties(itemID, lang);

        for(ClaimItem propId : props){

            System.out.println(propId);

        }


    }

    /**
     * Get aliases of a wikidata item
     * @param itemID
     * @param lang
     * @return
     */
    public static List<String> getItemAliases(String itemID, String lang){

        List<String> alisasList = null;

        try{
            GetField aliasesField = r.table(main_table).get(itemID).field("aliases").field(lang).field("value");

            alisasList = (List<String>) aliasesField.run(con);
        }
        catch(RethinkDBException e){
           //"No description is available for this language
            return null;

        }


        return alisasList;
    }

    /**
     * Get the description of a wikidata item
     * @param itemID
     * @param lang
     * @return
     */
    public static String getItemDescriptions(String itemID, String lang){

        String description = null;

        try{
            GetField descField = r.table(main_table).get(itemID).field("descriptions").field(lang).field("value");

            description = (String) descField.run(con);
        }
        catch(RethinkDBException e){

//            return "No description is available for " + lang;
            return null ;

        }

        return description;
    }

    /**
     * Get the label of wikidata item
     * @param itemID
     * @param lang
     * @return
     */
    public static String getItemLabels(String itemID, String lang){
        String label = null;
        try{
            GetField labelField = r.table(main_table).get(itemID).field("labels").field(lang).field("value");
            label = (String) labelField.run(con);
        }
        catch(RethinkDBException e){

            //   return "No lable is available for " + lang;

            return null;

        }

        return label ;
    }

    /**
     * Get the properties and their values for a given wikidata item
     * @param itemdId
     * @param lang
     * @return
     */
    public static List<ClaimItem> getItemProperties(String itemdId, String lang){


        List<ClaimItem> cItemList = new ArrayList<ClaimItem>();


        Get q1 = r.table(main_table).get(itemdId);
        HashMap<String, List<Map>> propertyMap = null;
        try{
             propertyMap = (HashMap<String,List<Map>>) q1.field("claims").run(con);
        }
        catch(RethinkDBException e){

            //If there is no claims exit
            System.out.println("No claims were found for " + itemdId);
            return cItemList;
        }

        for(String propertyID: propertyMap.keySet()){

            List propertyEntries = propertyMap.get(propertyID);


            for (int i = 0; i < propertyEntries.size(); i++) {



                Map propertyEntry = (Map) propertyEntries.get(i);

                Map mainsnakMap = (Map) propertyEntry.get("mainsnak");

                //String dataType = (String) mainsnakMap.get("datatype");

                //The actual entry for the value of the property
                Map dataValueMap = (Map) mainsnakMap.get("datavalue");

                String dataValueType = null;
                Object dataValueActualValue  = null ;

                if(dataValueMap!=null){

                        //The value type of the property
                        dataValueType = (String) dataValueMap.get("type");

                        //The actual property value
                        dataValueActualValue = dataValueMap.get("value");
                }

                ClaimItem cItem = new ClaimItem();



                if(dataValueActualValue !=null) {

                    //In case the rage of the property is a wikidata entry obtain the corresponding value
                    if(dataValueType.equals("wikibase-entityid")){

                        Double relatedEntityID = (Double) ((Map)dataValueActualValue).get("numeric-id");
                        String relatedEntityFinalId = "Q"+ new DecimalFormat("#").format(relatedEntityID);

                        String finalValue = getItemLabels(relatedEntityFinalId,lang);

                        //Ignore the property if its value is not defined
                        if(finalValue==null){
                            continue;
                        }
                        cItem.setPropertyValue(finalValue);
                    }
                    else{ //Otherwise use the raw value of the property
                        cItem.setPropertyValue(dataValueActualValue.toString());
                    }

                }

                cItem.setPropertyID(propertyID);
                cItem.setPropertyName( getItemLabels(propertyID,lang));
                cItem.setPropertyDataType(dataValueType);
                cItemList.add(cItem);
            }
        }

        return cItemList;
    }


    /**
     * Get the number of stored documents
     * @param databseName
     * @param tableName
     * @return
     */
    public static int getNrOfEntites(String databseName, String tableName){
        con.use(databseName);
        String count = r.table(main_table).count().run(con).toString();
        return Integer.valueOf(count);
    }


    public static void main(String[] args)
    {

        queryWikiData("Q2", "ar");

        System.out.println();
    }

    public static List<String> getDocs(int max){
//      con.use("test");

      List<String> entityIdList = new ArrayList<String>();
      String[] ignoredFields = {"aliases","claims","labels","descriptions", "datatype","type"};
//      List<Map<String, String>> res = (List<Map<String, String>>) r.table(main_table).without(ignoredFields).limit(max).run(con);

//      RqlUtil.toRqlQuery("r.row('id').match(\"^Q1\")");
//      RqlQuery query = RqlUtil.toRqlQuery("r.row('id').match('^Q1')");
//
//      System.out.println(query.run(con));
//
//      System.out.println(r.table(main_table).filter(query).without(ignoredFields).limit(3).run(con));

      List<Map<String, String>> res = (List<Map<String, String>>) r.table(main_table).without(ignoredFields).limit(3).run(con);


      for(Map<String, String> a : res){


          entityIdList.add(a.get("id"));

      }

      return entityIdList;

  }
}
