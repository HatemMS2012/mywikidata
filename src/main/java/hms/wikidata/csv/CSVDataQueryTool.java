package hms.wikidata.csv;

import hms.wikidata.dbimport.JacksonDBAPI;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class CSVDataQueryTool {

	
	
	public static void getItemLablesForCSV(String itemFile, String itemLabelFile, String lang){
		
		
		LineIterator it = null;
		try {
			
			PrintWriter out = new PrintWriter(new File(itemLabelFile));
			out.println("Reference Item \t Label \t count");

			it = FileUtils.lineIterator(new File(itemFile), "UTF-8");
			while (it.hasNext()) {
				

				String lineArr[] = it.nextLine().split(",");
				String refId = lineArr[0];
				String count = lineArr[1];
				
				String label = JacksonDBAPI.getItemLabel(refId, lang);
			
				out.println(refId + "\t" + label + "\t" + count);
				System.out.println(refId + "\t" + label + "\t" + count);
			}
			out.close();
			System.out.println("Finished processing");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		} finally {
			LineIterator.closeQuietly(it);
		}

		
	}
	
	public static void getReferenceCounts(String refItemFile, String refItemCountFile){
		
		LineIterator it = null;
		try {
			
			PrintWriter out = new PrintWriter(new File(refItemCountFile));
			out.println("Reference Item \t Label ");

			it = FileUtils.lineIterator(new File(refItemFile), "UTF-8");
			
			while (it.hasNext()) {
				
				
				String refId = it.nextLine().split(",")[0];
				
				System.out.println("Counting occurrences of [" + refId + "]");
				int count  = JacksonDBAPI.getReferenceOccurrenceCount(refId, "wikibase-item");
			
				out.println(refId + "\t" + count);
				System.out.println(refId + "\t" + count);
			}
			out.close();
			System.out.println("Finished processing");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		} finally {
			LineIterator.closeQuietly(it);
		}
		
	}
	
	public static void main(String[] args) {
		
//		getItemLablesForCSV("C:/Users/mousselly/Documents/Wikidata/Data/distinct_references.csv", "output_csv/reference_items_lables.txt", "en");
		
		getItemLablesForCSV("C:/Users/mousselly/Documents/Wikidata/Data/reference_usage_count.csv", "output_csv/reference_usage_count_labels.txt","en");
	}
}
