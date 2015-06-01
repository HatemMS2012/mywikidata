package hms.wikidata.carrot2;

import hms.wikidata.dbimport.JacksonDBAPI;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * <?xml version="1.0" encoding="UTF-8"?>
<searchresult>
  <query>Globe</query>
  <document id="P1001">
    <title>default</title>
    <url>111</url>
    <snippet>
      applies to jurisdiction the item (an institution, law, ...) belongs to or applies to the value (a jurisdiction: a country, state, municipality, ...)
    </snippet>
  </document>
  <document id="1">
    <title>Skate Shoes by Globe | Time For Change</title>
    <url>http://www.globeshoes.com/</url>
    <snippet>
      Skaters, surfers, and showboarders
      designing in their own style.
    </snippet>
  </document>
  
 </searchresult>
 *
 */
public class Carrot2Converter {

	
	
	private static DocumentBuilderFactory docFactory ;
	private static DocumentBuilder docBuilder ;
	private static  Document xmlDocument ;
	// root elements

	
	public static void convertToCarrotXML( List<WikidataEnitiy> wdEnitiyList, String output){
		docFactory = DocumentBuilderFactory.newInstance();
		try {
			docBuilder = docFactory.newDocumentBuilder();
			xmlDocument = docBuilder.newDocument();
			Element rootElement = xmlDocument.createElement("searchresult");
		 	xmlDocument.appendChild(rootElement);
		 	
		 	
		 	Element query = xmlDocument.createElement("query");
			query.appendChild(xmlDocument.createTextNode(""));
			rootElement.appendChild(query);
			
			for(WikidataEnitiy wde : wdEnitiyList) {
			
				Element document = xmlDocument.createElement("document");
				Attr attr = xmlDocument.createAttribute("id");
				attr.setValue(wde.getId());
				document.setAttributeNode(attr);
				rootElement.appendChild(document);
				
				Element title = xmlDocument.createElement("title");
				title.appendChild(xmlDocument.createTextNode(wde.getLabel()));
				document.appendChild(title);
				

				Element snippet = xmlDocument.createElement("snippet");
				snippet.appendChild(xmlDocument.createTextNode(wde.getDescription()));
				document.appendChild(snippet);
			
			}
			
		 	
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(new File(output));
			transformer.transform(source, result);
			 
			System.out.println("File saved!");
			// Output to console for testing
			StreamResult resultOut = new StreamResult(System.out);
			transformer.transform(source, resultOut);
			 
			
		 	
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	public static List<WikidataEnitiy> extractWDEntities(String inputFile, Set<String> allowedTypes){
		 
		
		List<WikidataEnitiy> wikidataEntityList = new ArrayList<WikidataEnitiy>();
		
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\t";
	 
		int i = 0;
		try {
	 
			br = new BufferedReader(new FileReader(inputFile));
			while ((line = br.readLine()) != null) {
				
				if(i==0){
					i++;
					continue;
				}
				
				i++;
	 
			    // use comma as separator
				//propId	label	description	label.desc	count	type	cluster id
				String[] lineAsArr = line.split(cvsSplitBy);
				String id = lineAsArr[0];
				String label = lineAsArr[1];
				String desc = lineAsArr[2];
				String aliases = lineAsArr[3];
				String type = lineAsArr[4];//.trim().toLowerCase().replace(" ", "");

				if(desc ==null){
					desc = "";
							
				}
				if(aliases ==null){
					aliases = "";
							
				}
				
				if(allowedTypes == null || allowedTypes.contains(type)){
					WikidataEnitiy wdEntity = new WikidataEnitiy();
					wdEntity.setId(id);
					wdEntity.setLabel(label+ " , " + aliases + " : " + desc);
					//wdEntity.setDescription(desc);
					wdEntity.setDescription("");
					wdEntity.setAliases(aliases);
					wikidataEntityList.add(wdEntity);
				}
				
			}
	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("#Total processed: " + i);
		
		System.out.println("#Total properties: " + wikidataEntityList.size());
		
		return wikidataEntityList;
	}
	
	//properties_statitics_title_aliases_desc_no_snippts.xml.xml
	public static void convertClusterOutputToCSV(String xmlfile, String output) throws FileNotFoundException{
		
		PrintWriter out = new PrintWriter(new File(output));
		out.println("id \t label \t desc \t aliases \t type \t cluster");
		File fXmlFile = new File(xmlfile);
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			
			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			
			NodeList nList = doc.getElementsByTagName("group");
			
			for (int temp = 0; temp < nList.getLength(); temp++) {
				 
				 Node nNode = nList.item(temp);
				 
				 if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					 
						Element eElement = (Element) nNode;
			 
						Element titleElement = (Element) eElement.getElementsByTagName("title").item(0);
						String clusterName = titleElement.getElementsByTagName("phrase").item(0).getTextContent();
						System.out.println(clusterName);
						
						NodeList docNodeList = eElement.getElementsByTagName("document");
						for (int i = 0; i < docNodeList.getLength(); i++) {
							
							
							Element docElement = (Element) docNodeList.item(i);
							
							String id = docElement.getAttribute("refid");
							
							
							
							String label = JacksonDBAPI.getItemLabel(id, "en");
							String desc =  JacksonDBAPI.getItemDescription(id, "en");
							String aliases = JacksonDBAPI.getItemAliases(id, "en").toString().replace("[", "").replace("]", "");
							String type = JacksonDBAPI.getPropertyType(id);
							
							
							out.println(id + "\t" + label + "\t" + desc + "\t" + aliases + "\t" + type + "\t" +clusterName );
						}
					}
				
			}
			out.close();

			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	//properties_statitics_title_aliases_desc_no_snippts.xml.xml
		public static Map<String, List<String>> extractPropertiesClusterLabels(String xmlfile) {
			

			Map<String, List<String>> propertyLabelMap = new HashMap<String, List<String>>();
			
			File fXmlFile = new File(xmlfile);
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder;
			try {
				dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fXmlFile);
				
				System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
				
				NodeList nList = doc.getElementsByTagName("group");
				
				for (int temp = 0; temp < nList.getLength(); temp++) {
					 
					 Node nNode = nList.item(temp);
					 
					 if (nNode.getNodeType() == Node.ELEMENT_NODE) {
						 
							Element eElement = (Element) nNode;
				 
							Element titleElement = (Element) eElement.getElementsByTagName("title").item(0);
							String clusterName = titleElement.getElementsByTagName("phrase").item(0).getTextContent();
							System.out.println(clusterName);
							
							NodeList docNodeList = eElement.getElementsByTagName("document");
							for (int i = 0; i < docNodeList.getLength(); i++) {
								
								
								Element docElement = (Element) docNodeList.item(i);
								
								String id = docElement.getAttribute("refid");
							
								if(propertyLabelMap.get(id)==null){
									
									List<String> labelList = new ArrayList<String>();
									labelList.add(clusterName);
									propertyLabelMap.put(id, labelList);
								}
								else{
									
									List<String> labelList = propertyLabelMap.get(id);
									labelList.add(clusterName);
									propertyLabelMap.put(id, labelList);
								}
								
							}
						}
					
				}
				

			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return propertyLabelMap;
			
		}
		
		public static void writePropertyClusterAsCsv(String clusterOutput, String csvfile){
			
			Map<String, List<String>> propLabels = extractPropertiesClusterLabels(clusterOutput);
			
			
			try {
				PrintWriter out = new PrintWriter(new File(csvfile));
				out.println("id \t label \t desc \t aliases \t type \t cluster_labels");

				for(String propId : propLabels.keySet()){
					
					String clusterLabels = propLabels. get(propId).toString().replace("[", "").replace("]", "");
					
					String label = JacksonDBAPI.getItemLabel(propId, "en");
					String desc =  JacksonDBAPI.getItemDescription(propId, "en");
					String aliases = JacksonDBAPI.getItemAliases(propId, "en").toString().replace("[", "").replace("]", "");
					String type = JacksonDBAPI.getPropertyType(propId);
					
					
					out.println(propId + "\t" + label + "\t" + desc + "\t" + aliases + "\t" + type + "\t" +clusterLabels );
							
					
				}
				out.close();
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
	
	public static void main(String[] args) throws FileNotFoundException {
		
		Set<String> allowedTypes = new HashSet<String>();
//		allowedTypes.add("monolingualtext");
//		allowedTypes.add("quantity");
//		allowedTypes.add("wikibase-item");
//		allowedTypes.add("wikibase-property");

//		List<WikidataEnitiy> wdeList = extractWDEntities("output/property_analysis_wikibase_type.txt",null);
		
//		convertToCarrotXML(wdeList, "output/property_analysis_wikibase_type.xml");
		
		writePropertyClusterAsCsv("carrot_clusters/property_analysis_wikibase_type_no_subject.xml", "carrot_clusters/labeled_properties_no_sub.txt");
		
//		convertClusterOutputToCSV("output/properties_statitics_title_aliases_desc_no_snippts.xml.xml", "output/carrot_clusters.txt");
		
	}
	
}
