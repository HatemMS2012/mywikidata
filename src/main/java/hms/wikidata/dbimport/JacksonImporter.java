package hms.wikidata.dbimport;

import hms.wikidata.model.MetaDataFields;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.RuntimeErrorException;
import javax.swing.text.html.parser.Entity;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonImporter {
	
	private static final String JSON_FILE_LOCATION = "C:/Devlopement/Dumps/20150420.json";
	

	
	
	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException, SQLException {
	
		
//		extractAndInsertIntoDB(MetaDataFields.descriptions);
//		extractAndInsertIntoDB(MetaDataFields.aliases);
//		parseWikidata(JSON_FILE_LOCATION);
//		extractAndInsertClaims();
		
		
		extractAndInsertClaimsReferences();
		
	}
	
	

	
	public static void extractAndInsertIntoDB(MetaDataFields metaDataFieldName) throws JsonParseException, IOException{
		
		//Load the ids of already inserted elements into memory for efficient processing
		Set<String> alreadyInDB =  null;
		if(metaDataFieldName.equals(MetaDataFields.descriptions)){
			alreadyInDB = WikidataToRDB.getEntitiesWithDescription();
			
		}
		else if (metaDataFieldName.equals(MetaDataFields.labels)){
			alreadyInDB = WikidataToRDB.getEntitiesWithLabels();
			
		}
		else if (metaDataFieldName.equals(MetaDataFields.aliases)){
			alreadyInDB = WikidataToRDB.getEntitiesWithAliases();
			
		}
		
		System.out.println("There is already " + alreadyInDB.size() + " enties in the description table");
		
		//Parse the JSON file
		JsonFactory f = new MappingJsonFactory();
		JsonParser jp = f.createParser(new File(JSON_FILE_LOCATION));
		JsonToken current = jp.nextToken();

		int counter = 0 ;
		if (current == JsonToken.START_ARRAY) {
			// For each of the records in the array
			while (jp.nextToken() != JsonToken.END_ARRAY) {
				// read the record into a tree model, this moves the parsing position to the end of it
				JsonNode node = jp.readValueAsTree();
				String itemID = node.get("id").asText();
			
				//skip already inserted elements
				if(alreadyInDB.contains(itemID)){
					System.out.println("skipping: " + itemID + " : " + counter++);
					continue;
				}
				
//				alreadyInDB=null; //for garbage collection
				
				JsonNode metaFileNode = node.get(metaDataFieldName.toString());
				
				if(metaFileNode != null){
					

					for (Iterator<JsonNode> iterator = metaFileNode.elements(); iterator.hasNext();) {
						
						
						JsonNode metaFieldElements =  iterator.next();
						
						if(metaDataFieldName.equals(MetaDataFields.aliases)){
							
							for (Iterator<JsonNode> iterator2 = metaFieldElements.elements(); iterator2.hasNext();) {
								
								JsonNode metaFieldElementsOfElements =  iterator2.next();
								
								handleMetafieldElement(metaDataFieldName, itemID, metaFieldElementsOfElements);

							}
							
						}
		
						else{
							handleMetafieldElement(metaDataFieldName, itemID, metaFieldElements);
						}
					}
				}
			}
		}	
	}




	private static void handleMetafieldElement(
			MetaDataFields metaDataFieldName, String itemID,
			JsonNode metaFieldElements) {
		JsonNode metaFielElement = metaFieldElements.get("value");
		
		
		if(metaFielElement != null) {
				
	
			
			String lang = metaFieldElements.get("language").asText();
			
			
			String metaFieldValue = metaFieldElements.get("value").asText();
			
			System.out.println("Insert " + metaDataFieldName +": " + itemID + "\t" + metaFieldValue + "\t" + lang );

			try{
				
				if(metaDataFieldName.equals(MetaDataFields.descriptions)){
					
					WikidataToRDB.insertDescription(itemID, lang, metaFieldValue);
					
				}
				else if (metaDataFieldName.equals(MetaDataFields.labels)){
					WikidataToRDB.insertLabel(itemID, lang, metaFieldValue);
					
				}
				else if (metaDataFieldName.equals(MetaDataFields.aliases)){
					WikidataToRDB.insertAlias(itemID, lang, metaFieldValue);
					
				}
			}
			catch(SQLException e) {
				
				if(e.getMessage().contains("Incorrect string value")){
					System.err.println("Cannot insert: " + metaFieldValue + "@" + lang);
				}
				else {
					e.printStackTrace();
				}
			}
		}
	}

	
	
	public static void extractAndInsertIds() throws JsonParseException, IOException, SQLException{
		
		
		JsonFactory f = new MappingJsonFactory();

		JsonParser jp = f.createParser(new File(JSON_FILE_LOCATION));

		JsonToken current;

		current = jp.nextToken();
		System.out.println(current);

		if (current == JsonToken.START_ARRAY) {
			// For each of the records in the array

			int counter = 0 ;
			while (jp.nextToken() != JsonToken.END_ARRAY) {
				// read the record into a tree model,
				// this moves the parsing position to the end of it
				JsonNode node = jp.readValueAsTree();
				
				String itemID = node.get("id").asText();
				String type = node.get("type").asText();

				WikidataToRDB.insertItem(itemID,  type);
				counter ++ ;
				System.out.println(counter + ": INSERT[Item] " + itemID + "\t" +  "\t" + type );

				
			}
		}
		
	}
	
	public static void extractAndInsertClaims() throws IOException {

		JsonFactory f = new MappingJsonFactory();

		JsonParser jp = f.createParser(new File(JSON_FILE_LOCATION));

		JsonToken current;

		current = jp.nextToken();
		System.out.println(current);

		if (current == JsonToken.START_ARRAY) {
			// For each of the records in the array
			while (jp.nextToken() != JsonToken.END_ARRAY) {
				// read the record into a tree model,
				// this moves the parsing position to the end of it
				JsonNode node = jp.readValueAsTree();
				String itemID = node.get("id").asText();
				
				JsonNode claimsNode = node.get("claims");
				
				if(claimsNode!=null){
					for (Iterator<JsonNode> iterator = claimsNode.elements(); iterator.hasNext();) { 
					
						JsonNode singleClaim =  iterator.next();
						
						for (Iterator<JsonNode> iterator2 = singleClaim.elements(); iterator2.hasNext();) {	 
							
							JsonNode content = iterator2.next();
							JsonNode ms = content.get("mainsnak");
							JsonNode propetyId = ms.get("property");
							JsonNode datatype = ms.get("datatype");
							//Some attributes don't have a value "novalue"
							if(datatype == null){
								continue;
							}
							JsonNode datavalueNode = ms.get("datavalue");
						
							String finalValue = null;
							
							if(datatype.asText().equals("wikibase-item")){
							
								finalValue = "Q"+datavalueNode.get("value").get("numeric-id").asText();
								
							}
							
							else if(datatype.asText().equals("time")){
								finalValue = datavalueNode.get("value").toString();
							}
							else if(datatype.asText().equals("wikibase-property")){
								finalValue = "P"+datavalueNode.get("value").get("numeric-id");
							}
							
							else if(datatype.asText().equals("globe-coordinate")){
								finalValue = datavalueNode.get("value").toString();
							}
							
							else if(datatype.asText().equals("quantity")){
								finalValue = datavalueNode.get("value").toString();
							}
							else if(datatype.asText().equals("monolingualtext")){
								finalValue = datavalueNode.get("value").toString();
							}
							else if(datatype.asText().equals("string")){
								finalValue = datavalueNode.get("value").toString();
							}
							else if(datatype.asText().equals("commonsMedia")){
								finalValue = datavalueNode.get("value").toString();
							}
							else if(datatype.asText().equals("url")){
								finalValue = datavalueNode.get("value").asText();
							}
							else {
								finalValue = datavalueNode.get("value").toString();	
								
							}
							
												
							try {
								System.out.println("INSERT[Claim] " + itemID + "\t" +  "\t" + propetyId );
	
								WikidataToRDB.insertClaim(itemID, propetyId.asText(), finalValue, datatype.asText());
							} catch (SQLException e) {
								if(e.getMessage().contains("Incorrect string value")){
									System.err.println("Cannot insert: " +  finalValue);
								}
								else 
									e.printStackTrace();
							}
						}					
						
					}
					
				}
			}
		}
	}
	
	
	public static void extractAndInsertClaimsReferences() throws IOException {
		System.out.println("item id \t claim id \t reference id \t reference type \t reference value");

		JsonFactory f = new MappingJsonFactory();

		JsonParser jp = f.createParser(new File(JSON_FILE_LOCATION));

		JsonToken current;

		current = jp.nextToken();
		System.out.println(current);

		if (current == JsonToken.START_ARRAY) {
			// For each of the records in the array
			while (jp.nextToken() != JsonToken.END_ARRAY) {
				// read the record into a tree model,
				// this moves the parsing position to the end of it
				JsonNode node = jp.readValueAsTree();
				
				String itemID = node.get("id").asText();
				
				JsonNode claimsNode = node.get("claims");
				
				if(claimsNode!=null){
					for (Iterator<JsonNode> iterator = claimsNode.elements(); iterator.hasNext();) { 
					
						JsonNode singleClaim =  iterator.next();
						
						
						for (Iterator<JsonNode> claimIterator = singleClaim.elements(); claimIterator.hasNext();) {	 
							
							JsonNode content = claimIterator.next();
							JsonNode ms = content.get("mainsnak");
						
							String claimId = ms.get("property").asText();
							
							JsonNode datatypeNode = ms.get("datatype");
							//Some attributes don't have a value "novalue"
							if(datatypeNode == null){
								continue;
							}
							
							String datatype = datatypeNode.asText();
							
							JsonNode datavalueNode = ms.get("datavalue");
						
							String targetOfClaim = null;
							
							if(datatype.equals("wikibase-item")){
							
								targetOfClaim = "Q"+datavalueNode.get("value").get("numeric-id").asText();
								
							}
							
							else if(datatype.equals("time")){
								targetOfClaim = datavalueNode.get("value").toString();
							}
							else if(datatype.equals("wikibase-property")){
								targetOfClaim = "P"+datavalueNode.get("value").get("numeric-id");
							}
							
							else if(datatype.equals("globe-coordinate")){
								targetOfClaim = datavalueNode.get("value").toString();
							}
							
							else if(datatype.equals("quantity")){
								targetOfClaim = datavalueNode.get("value").toString();
							}
							else if(datatype.equals("monolingualtext")){
								targetOfClaim = datavalueNode.get("value").toString();
							}
							else if(datatype.equals("string")){
								targetOfClaim = datavalueNode.get("value").toString();
							}
							else if(datatype.equals("commonsMedia")){
								targetOfClaim = datavalueNode.get("value").toString();
							}
							else if(datatype.equals("url")){
								targetOfClaim = datavalueNode.get("value").asText();
							}
							else {
								targetOfClaim = datavalueNode.get("value").toString();	
								
							}
							
							
							JsonNode referencesNode = content.get("references");
							
							
							if(referencesNode!=null){
							//For each reference
							
								for (Iterator<JsonNode> refIterator = referencesNode.elements(); refIterator.hasNext();) {	 
									
										JsonNode referenceNode = refIterator.next();
										
										
										JsonNode refSnaksNode = referenceNode.get("snaks");
										
										String refHash = referenceNode.get("hash").asText();
										
										//for each reference snaks node
										
										for (Iterator<JsonNode> refSnaksIterator = refSnaksNode.elements(); refSnaksIterator.hasNext();) {	
											
											JsonNode refSanksItems = refSnaksIterator.next();
											
										
											
											for (Iterator<JsonNode> refSanksItemsIterator = refSanksItems.elements(); refSanksItemsIterator.hasNext();) {	
												
											
												
												JsonNode ttt = refSanksItemsIterator.next();
												String snakType = ttt.get("snaktype").asText();
												String property = ttt.get("property").asText();


												
												if(snakType.equals("value")){
													
													datatype = ttt.get("datatype").asText();

													String value = null ;
												
													if(datatype.equals("wikibase-item")){
														value = "Q"+ttt.get("datavalue").get("value").get("numeric-id").asText();
													}
													else if(datatype.equals("time")){
														value = ttt.get("datavalue").get("value").toString();
													}
													else if(datatype.equals("wikibase-property")){
														value = "P"+ttt.get("datavalue").get("value").get("numeric-id");
													}
													
													else if(datatype.equals("globe-coordinate")){
														value = ttt.get("datavalue").get("value").toString();
													}
													
													else if(datatype.equals("quantity")){
														value = ttt.get("datavalue").get("value").toString();
													}
													else if(datatype.equals("monolingualtext")){
														value = ttt.get("datavalue").get("value").toString();
													}
													else if(datatype.equals("string")){
														value = ttt.get("datavalue").get("value").toString();
													}
													else if(datatype.equals("commonsMedia")){
														value = ttt.get("datavalue").get("value").toString();
													}
													else if(datatype.equals("url")){
														value = ttt.get("datavalue").get("value").asText();
													}
													else {
														value = ttt.get("datavalue").get("value").toString();
														
													}
													
													System.out.println(itemID + "\t" + claimId + "\t" + targetOfClaim + "\t" + property + "\t" + datatype + "\t" + value + "\t" + refHash );

													try {
														WikidataToRDB.insertClaimReference(itemID, claimId, targetOfClaim, property, datatype, value, refHash);
													} catch (SQLException e) {
														// TODO Auto-generated catch block
														e.printStackTrace();
														throw new RuntimeException();
													}

												
												}
												else{
													
													System.err.println(itemID + "\t" + claimId + "\t" + property + "\t" + null + "\t" + "no-value \t" + refHash );

												}
												
												
		
											}
											
										}
										
										
										
								}
							}
				
						}					
						
					}
					
				}
			}
		}
	}
	/**
//	 * extract 
//	 * @throws JsonParseException
//	 * @throws IOException
//	 * @throws SQLException
//	 */
//	public static void extractAndInsertLabels() throws JsonParseException, IOException, SQLException{
//		
//
//		Set<String> alreadyInDB = WikidataToRDB.getEntitiesWithLabels();
//		System.out.println("There is already " + alreadyInDB.size() + " enties in the label table");
//
//		JsonFactory f = new MappingJsonFactory();
//
//		JsonParser jp = f.createParser(new File(JSON_FILE_LOCATION));
//
//		JsonToken current;
//
//		current = jp.nextToken();
//		System.out.println(current);
//
//		if (current == JsonToken.START_ARRAY) {
//			// For each of the records in the array
//			while (jp.nextToken() != JsonToken.END_ARRAY) {
//				
//			
//				
//				// read the record into a tree model,
//				// this moves the parsing position to the end of it
//				JsonNode node = jp.readValueAsTree();
//				
//		
//				String itemID = node.get("id").asText();
//				
//				//if(WikidataToRDB.isLabelEntityAlreadyInDB(itemID)){
//			
//				if(alreadyInDB.contains(itemID)){
//					System.out.println("skipping: " + itemID);
//					continue;
//				}
//					
//				
//				JsonNode labelNode = node.get("labels");
//				if(labelNode==null)
//					continue;
//				
//				for (Iterator<JsonNode> iterator = labelNode.elements(); iterator.hasNext();) {
//					
//					
//					
//					JsonNode labelElements =  iterator.next();
//					String lang = labelElements.get("language").asText();
//
//					if( lang.equals("got")){
//						continue;
//					}
////					if(WikidataToRDB.isLabelAlreadyInDB(itemID, lang)){
////						
////						continue;
////					}
//					
//					JsonNode labelElement = labelElements.get("value");
//					if(labelElement == null)
//						continue;
//					
//					String label = labelElements.get("value").asText();
//					
////					if(itemID.equals("Q302494") && lang.equals("zh-hans")){
////						
////						continue;
////					}
////					if(itemID.equals("Q84031") && lang.equals("zh-cn")){
////						
////						continue;
////					}
////					if(lang.contains("zh-")){
////						
////						continue;
////					}
////					if(itemID.equals("Q134418") && lang.contains("zh-")){
////						
////						continue;
////					}
////					
////					if(itemID.equals("Q42320") && lang.equals("ca")){
////						continue;
////					}
////					if( lang.equals("got") || label.equals("肋𩩍骨") ||   label.equals("𨧀") || label.equals("𢆡罩")  || label.equals("鄭克𡒉") ){
////						continue;
////					}
//					
//					
//					
////					if(lang.equals("zh")){
////
////						byte[] bytes = label.getBytes();
////						label = new String(bytes, Charset.forName("UTF-8") );
////					}
//					System.out.println("INSERT[label table] " + itemID + "\t" + label + "\t" + lang );
//
//					try{
//						
//						WikidataToRDB.insertLabel(itemID, lang, label);
//					}
//					catch(SQLException e) {
//						
//						e.getMessage().contains("Incorrect string value");
//						System.err.println("Cannot insert: " + label + "@" + lang);
//					}
//					
//					
//					
//					
//				}
//			}
//		}
//		
//	}
//
//	public static void extractAndInsertDescriptions() throws JsonParseException, IOException, SQLException{
//		
//		Set<String> alreadyInDB = WikidataToRDB.getEntitiesWithDescription();
//		System.out.println("There is already " + alreadyInDB.size() + " enties in the description table");
//		
//		
//		JsonFactory f = new MappingJsonFactory();
//
//		JsonParser jp = f.createParser(new File(JSON_FILE_LOCATION));
//
//		JsonToken current;
//
//		current = jp.nextToken();
//		System.out.println(current);
//
//		if (current == JsonToken.START_ARRAY) {
//			// For each of the records in the array
//			while (jp.nextToken() != JsonToken.END_ARRAY) {
//				// read the record into a tree model,
//				// this moves the parsing position to the end of it
//				JsonNode node = jp.readValueAsTree();
//				String itemID = node.get("id").asText();
//			
//				if(alreadyInDB.contains(itemID)){
//					System.out.println("skipping: " + itemID);
//					continue;
//				}
//				
//				
//				JsonNode desclNode = node.get("descriptions");
//				
//				if(desclNode == null)
//					continue;
//
//				for (Iterator<JsonNode> iterator = desclNode.elements(); iterator.hasNext();) {
//					
//					
//					JsonNode descElements =  iterator.next();
//					
//					JsonNode descElement = descElements.get("value");
//					
//					if(descElement == null)
//						continue;
//					
//					String lang = descElements.get("language").asText();
//					
//					if(lang.equals("got")){
//						continue;
//					}
//					
//					String description = descElements.get("value").asText();
//					
//					System.out.println("INSERT[Tabel description] " + itemID + "\t" + description + "\t" + lang );
//
//					
//					try{
//						
//						WikidataToRDB.insertDescription(itemID, lang, description);
//					}
//					catch(SQLException e) {
//						
//						e.getMessage().contains("Incorrect string value");
//						System.err.println("Cannot insert: " + description + "@" + lang);
//					}
//				}
//			}
//		}
//		
//	}
//	

	public static void parseWikidata(String filePath)
			throws JsonParseException, JsonMappingException, IOException {

		JsonFactory f = new MappingJsonFactory();

		JsonParser jp = f.createParser(new File(filePath));

		JsonToken current;

		current = jp.nextToken();
		System.out.println(current);

		if (current == JsonToken.START_ARRAY) {
			// For each of the records in the array
			while (jp.nextToken() != JsonToken.END_ARRAY) {
				// read the record into a tree model,
				// this moves the parsing position to the end of it
				JsonNode node = jp.readValueAsTree();
//				 System.out.println(node);
//				System.out.println(node.get("id"));	
//				System.out.println(node.get("type"));
				System.out.println(node.get("descriptions").get("en").get("value"));
				System.out.println(node.get("aliases").get("en"));
				System.out.println(node.get("labels").get("en"));
				
				JsonNode claimsNode = node.get("claims");
				
				for (Iterator<JsonNode> iterator = claimsNode.elements(); iterator.hasNext();) { 
				
					JsonNode singleClaim =  iterator.next();
					
					for (Iterator<JsonNode> iterator2 = singleClaim.elements(); iterator2.hasNext();) { 
						JsonNode content = iterator2.next();
						JsonNode ms = content.get("mainsnak");
						JsonNode p = ms.get("property");
						JsonNode datatype = ms.get("datatype");
						JsonNode datavalueNode = ms.get("datavalue");
						String finalValue = null;
						if(datatype.asText().equals("string")){
							finalValue = datavalueNode.get("value").asText();	
						}
						else if(datatype.asText().equals("wikibase-item")){
							
							finalValue = "Q"+datavalueNode.get("value").get("numeric-id").asText();
							
						}
						
						JsonNode finalType = datavalueNode.get("type");
						
					
						System.out.println(p + "=\t" +finalValue );
					}					
					
				}
				
				
			}
		}
	}
//	
}
