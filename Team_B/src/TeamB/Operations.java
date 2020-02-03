package shruti;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
public class Operations implements Constants {
	
	public static String[] getLastElement(Map<Integer, Cell> data, int len) {
		String[] rec_string = new String[len];
		try {
			
			int count = data.size();
			for (Map.Entry<Integer, Cell> entry : data.entrySet()) 
			{
				Cell cell = entry.getValue();
				rec_string[0] = Integer.toString(cell.get_RowId());
				String data2[] = cell.get_Payload().get_Data();
				for (int i=0, j=1; i<data2.length; j++, i++) {
					rec_string[j] = data2[i];
				}
			}
				return rec_string;
			}
		catch (Exception e) {
			e.printStackTrace();
		}
			return rec_string;
	}
	
	
	public static void Insert(String tableName, String[] values) {

		

//		public class abstract xmlfile {
//
//			public static void main(String argv[]) {
//
//			  try {
//
//				DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
//				DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
//
//				// root elements
//				Document doc = docBuilder.newDocument();
//				Element rootElement = doc.createElement("davisbase_tables");
//				doc.appendChild(rootElement);
//
//				// staff elements
//				Element staff = doc.createElement("Table1");
//				rootElement.appendChild(staff);
//
//				// set attribute to staff element
////				Attr attr = doc.createAttribute("id");
////				attr.setValue("1");
////				staff.setAttributeNode(attr);
//
//				// shorten way
//				// staff.setAttribute("id", "1");
//
//				// firstname elements
//				Element firstname = doc.createElement("table_name");
//				
//				
//				firstname.appendChild(doc.createTextNode("yong"));
//				staff.appendChild(firstname);
//
//				// lastname elements
//				Element lastname = doc.createElement("column_name");
//				lastname.appendChild(doc.createTextNode("mook kim"));
//				staff.appendChild(lastname);
//
//				// nickname elements
//				Element nickname = doc.createElement("constraint");
//				nickname.appendChild(doc.createTextNode("mkyong"));
//				staff.appendChild(nickname);
//
//				// salary elements
//				Element salary = doc.createElement("values");
//				salary.appendChild(doc.createTextNode("100000"));
//				staff.appendChild(salary);
//
//				// write the content into xml file
//				TransformerFactory transformerFactory = TransformerFactory.newInstance();
//				Transformer transformer = transformerFactory.newTransformer();
//				DOMSource source = new DOMSource(doc);
//				StreamResult result = new StreamResult(new File("C:\\Users\\preet\\eclipse-workspace\\Xml\\file.xml"));
//
//				// Output to console for testing
//				// StreamResult result = new StreamResult(System.out);
//
//				transformer.transform(source, result);
//
//				System.out.println("File saved!");
//
//			  } catch (ParserConfigurationException pce) {
//				pce.printStackTrace();
//			  } catch (TransformerException tfe) {
//				tfe.printStackTrace();
//			  }
//			}
//		}
		
		
File file = new File("data/catalog/tables.xml");
		
		
		
			
		try {
			DocumentBuilderFactory docConstraints = DocumentBuilderFactory.newInstance();
			
			DocumentBuilder docConstraintsBuilder = docConstraints.newDocumentBuilder();
			
			Document doc = docConstraintsBuilder.parse(file);
			
			Element element = doc.getDocumentElement();
			
			
			//Element rootElement = doc.createElement("davisbase_tables");
			//doc.appendChild(rootElement);
			
			tableName=tableName.trim();
					
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			//int num_of_pages = (int) (table.length() / PAGESIZE);

			//Map<Integer, String> colNames = getColumnNames(tableName);
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Cell> columnsMeta = getStuff.getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = getStuff.getDataType(columnsMeta);
			String[] isNullable = getStuff.isNullable(columnsMeta);

			for (int i = 0; i < values.length; i++) {
				if (values[i].equalsIgnoreCase("null") && isNullable[i].equals("NO")) {
					System.out.println("Cannot insert NULL values in a field defined as NOT NULL");
					return;
				}
			}
			condition = new String[0];
			Map<Integer, Cell> data = getStuff.getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(values[0]))) {
				System.out.println("Duplicate value for primary key");
				return;
			}
			
			Map<Integer, String> colNames = getStuff.getColumnNames(tableName);
			//NodeList 
			NodeList nodeList = doc.getChildNodes();
			//NodeList node = nodeList.
			for (int i = 0; i < values.length; i++)
			{
				for(int j = 0; j < nodeList.getLength(); j++)
				{
					Element el = (Element)nodeList.item(j);
					if (el.hasChildNodes())
					{
						String tname = el.getElementsByTagName("table_id").item(0).getTextContent();
					
						String colname = el.getElementsByTagName("column_name").item(0).getTextContent();
						String constraintname = el.getElementsByTagName("constraint_name").item(0).getTextContent();
						String colvalue = el.getElementsByTagName("value").item(0).getTextContent();
						if(values[i].equalsIgnoreCase("null") && constraintname.equals("default"))
						{
							if(colname.equals(Integer.toString(i)))
							{
								values[i] = colvalue;
							}
						}
						else if(values[i].equalsIgnoreCase("null") && constraintname.equals("autoincrement"))
						{
							if(colname.equals(Integer.toString(i)))
							{
								String[] a = getLastElement(data,colNames.size());
								int auto = Integer.parseInt(a[i]);
								auto = auto+1;
								values[i] = Integer.toString(auto);
							}
						}
					}
					else
					{
						continue;
					}
				}
			}
			
			//get page number on which data exist
			int pageNo= getStuff.getPageNo(tableName,Integer.parseInt(values[0]));
			
			
			//check for duplicate value
			
			// check leaf size
			byte[] plDataType = new byte[dataType.length - 1];
			int payLoadSize = getStuff.getPayloadSize(tableName, values, plDataType, dataType);
			payLoadSize=payLoadSize+6;
			
			//change offset calculation??
			int address= Supplements.checkPageOverFlow(table,pageNo,payLoadSize);
			
			if(address!=-1){
				Cell cell= Supplements.createCell(pageNo,Integer.parseInt(values[0]),(short)payLoadSize,plDataType,values);
				Supplements.writePayload(table,cell,address);
			}
			else
			{
				Supplements.splitLeafPage(table,pageNo);
				int pNo=getStuff.getPageNo(tableName,Integer.parseInt(values[0]));
				int addr=Supplements.checkPageOverFlow(table,pNo,payLoadSize);
				Cell cell=Supplements.createCell(pNo,Integer.parseInt(values[0]),(short)payLoadSize,plDataType,values);
				Supplements.writePayload(table,cell,addr);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	

	

	public static void Query(String tableName, String[] columnNames, String[] condition) {

		try {

			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int num_of_pages = (int) (table.length() / PAGESIZE);

			Map<Integer, String> colNames = getStuff.getColumnNames(tableName);
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < num_of_pages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getStuff.getRecords(table, cellLocations,i);
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Cell> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				getStuff.printTable(colNames, filteredRecords);
			} else {
				getStuff.printTable(colNames, records);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static Map<Integer, Cell> filterRecords(Map<Integer, String> colNames, Map<Integer, Cell> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<Integer, Cell>();
		/*
		 * for (Map.Entry<Integer, String> entry : colNames.entrySet()) { String
		 * columnName=entry.getValue(); if(resultColumnSet.contains(columnName))
		 * colNames.remove(entry.getKey());
		 * //ordinalPosition.add(entry.getKey()); }
		 */
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {
			Cell cell = entry.getValue();
			Payload payload = cell.get_Payload();
			String[] data = payload.get_Data();
			byte[] dataTypeCodes = payload.get_DataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
	
	
	private static Map<Integer, Cell> filterRecords2(Map<Integer, String> colNames, Map<Integer, Cell> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<Integer, Cell>();
		/*
		 * for (Map.Entry<Integer, String> entry : colNames.entrySet()) { String
		 * columnName=entry.getValue(); if(resultColumnSet.contains(columnName))
		 * colNames.remove(entry.getKey());
		 * //ordinalPosition.add(entry.getKey()); }
		 */
		int whereOrdinalPosition = -1;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {
			Cell cell = entry.getValue();
			Payload payload = cell.get_Payload();
			String[] data = payload.get_Data();
			byte[] dataTypeCodes = payload.get_DataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
	
	private static Map<Integer, Cell> filterRecordsByData(Map<Integer, String> colNames, Map<Integer, Cell> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<Integer, Cell>();
		
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {
			Cell cell = entry.getValue();
			Payload payload = cell.get_Payload();
			String[] data = payload.get_Data();
			byte[] dataTypeCodes = payload.get_DataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
	
	private static boolean checkData(byte code, String data, String[] condition) {

		if (code >= 0x04 && code <= 0x07) {
			Long longValue = Long.parseLong(data);
			switch (condition[1]) {
			case "=":
				if (longValue == Long.parseLong(condition[2]))
					return true;
				break;
			case ">":
				if (longValue > Long.parseLong(condition[2]))
					return true;
				break;
			case "<":
				if (longValue < Long.parseLong(condition[2]))
					return true;
				break;
			case "<=":
				if (longValue <= Long.parseLong(condition[2]))
					return true;
				break;
			case ">=":
				if (longValue >= Long.parseLong(condition[2]))
					return true;
				break;
			case "<>":
				if (longValue != Long.parseLong(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code == 0x08 || code == 0x09) {
			Double doubleValue = Double.parseDouble(data);
			switch (condition[1]) {
			case "=":
				if (doubleValue == Double.parseDouble(condition[2]))
					return true;
				break;
			case ">":
				if (doubleValue > Double.parseDouble(condition[2]))
					return true;
				break;
			case "<":
				if (doubleValue < Double.parseDouble(condition[2]))
					return true;
				break;
			case "<=":
				if (doubleValue <= Double.parseDouble(condition[2]))
					return true;
				break;
			case ">=":
				if (doubleValue >= Double.parseDouble(condition[2]))
					return true;
				break;
			case "<>":
				if (doubleValue != Double.parseDouble(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code >= 0x0C) {

			condition[2] = condition[2].replaceAll("'", "");
			condition[2] = condition[2].replaceAll("\"", "");
			switch (condition[1]) {
			case "=":
				if (data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			case "<>":
				if (!data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}
		}

		return false;

	}

	public static void initializeDatabase() {

		File data = new File("data/catalog");
		File userData=new File("data/userdata");
		data.mkdir();
		userData.mkdir();
		RandomAccessFile tablesMeta;
		RandomAccessFile davisbaseColumnsCatalog;

		createDavisBase_Tables();
		createDavisBase_Columns();

	}

	public static void createDavisBase_Tables() {

		try {
			RandomAccessFile tablesMeta = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			tablesMeta.setLength(PAGESIZE * 1);
			tablesMeta.seek(0);
			tablesMeta.write(0x0D);
			tablesMeta.write(0x02);
			tablesMeta.writeShort(PAGESIZE - 32 - 33);
			tablesMeta.writeInt(-1);// rightmost
			tablesMeta.writeShort(PAGESIZE - 32);
			tablesMeta.writeShort(PAGESIZE - 32 - 33);

			tablesMeta.seek(PAGESIZE - 32);
			tablesMeta.writeShort(26);
			tablesMeta.writeInt(1);
			tablesMeta.writeByte(3);
			tablesMeta.writeByte(28);
			tablesMeta.write(0x06);
			tablesMeta.write(0x05);
			tablesMeta.writeBytes("davisbase_tables");
			tablesMeta.writeInt(2);
			tablesMeta.writeShort(34); // avg_length

			tablesMeta.seek(PAGESIZE - 32 - 33);
			tablesMeta.writeShort(19);
			tablesMeta.writeInt(2);
			tablesMeta.writeByte(3);
			tablesMeta.writeByte(29);
			tablesMeta.write(0x06);
			tablesMeta.write(0x05);
			tablesMeta.writeBytes("davisbase_columns");
			tablesMeta.writeInt(10);
			tablesMeta.writeShort(34); // avg_lngth

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void createDavisBase_Columns() {

		int cellHeader = 6;
		try {
			RandomAccessFile columnsMeta = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			columnsMeta.setLength(PAGESIZE * 1);
			columnsMeta.seek(0);
			columnsMeta.write(0x0D);
			columnsMeta.write(10);

			int recordSize[] = new int[] { 33, 39, 40, 43, 34, 40, 41, 39, 49, 41 };
			int offset[] = new int[10];

			offset[0] = PAGESIZE - recordSize[0] - cellHeader;

			// error
			columnsMeta.seek(4);

			columnsMeta.writeInt(-1);

			//columnsMeta.writeShort(offset[0]);
			for (int i = 1; i < offset.length; i++) {
				offset[i] = offset[i - 1] - (recordSize[i] + cellHeader);

			}
			columnsMeta.seek(2);
			columnsMeta.writeShort(offset[9]);

			columnsMeta.seek(8);
			for (int i = 0; i < offset.length; i++) {
				columnsMeta.writeShort(offset[i]);
			}

			// 1
			columnsMeta.seek(offset[0]);
			columnsMeta.writeShort(recordSize[0]);
			columnsMeta.writeInt(1);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(17);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("rowid");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(1);
			columnsMeta.writeBytes("NO");

			// 2
			columnsMeta.seek(offset[1]);
			columnsMeta.writeShort(recordSize[1]);
			columnsMeta.writeInt(2);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(22);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("table_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(2);
			columnsMeta.writeBytes("NO");

			// 3
			columnsMeta.seek(offset[2]);
			columnsMeta.writeShort(recordSize[2]);
			columnsMeta.writeInt(3);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(24);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("record_count");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(3);
			columnsMeta.writeBytes("NO");

			// 4
			columnsMeta.seek(offset[3]);
			columnsMeta.writeShort(recordSize[3]);
			columnsMeta.writeInt(4);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(22);
			columnsMeta.write(20);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("avg_length");
			columnsMeta.writeBytes("SMALLINT");
			columnsMeta.write(4);
			columnsMeta.writeBytes("NO");

			// 5
			columnsMeta.seek(offset[4]);
			columnsMeta.writeShort(recordSize[4]);
			columnsMeta.writeInt(5);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(17);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("rowid");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(1);
			columnsMeta.writeBytes("NO");

			// 6
			columnsMeta.seek(offset[5]);
			columnsMeta.writeShort(recordSize[5]);
			columnsMeta.writeInt(6);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(22);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("table_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(2);
			columnsMeta.writeBytes("NO");

			// 7
			columnsMeta.seek(offset[6]);
			columnsMeta.writeShort(recordSize[6]);
			columnsMeta.writeInt(7);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(23);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("column_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(3);
			columnsMeta.writeBytes("NO");

			// 8
			columnsMeta.seek(offset[7]);
			columnsMeta.writeShort(recordSize[7]);
			columnsMeta.writeInt(8);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(21);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("data_type");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(4);
			columnsMeta.writeBytes("NO");

			// 9
			columnsMeta.seek(offset[8]);
			columnsMeta.writeShort(recordSize[8]);
			columnsMeta.writeInt(9);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(28);
			columnsMeta.write(19);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("ordinal_position");
			columnsMeta.writeBytes("TINYINT");
			columnsMeta.write(5);
			columnsMeta.writeBytes("NO");

			// 10
			columnsMeta.seek(offset[9]);
			columnsMeta.writeShort(recordSize[9]);
			columnsMeta.writeInt(10);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(23);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("is_nullable");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(6);
			columnsMeta.writeBytes("NO");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String[] parseCondition(String whereCondition) {

		String condition[] = new String[3];
		String values[] = new String[2];
		if (whereCondition.contains("=")) {
			values = whereCondition.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains(">")) {
			values = whereCondition.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<")) {
			values = whereCondition.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains(">=")) {
			values = whereCondition.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<=")) {
			values = whereCondition.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<>")) {
			values = whereCondition.split("<>");
			condition[0] = values[0].trim();
			condition[1] = "<>";
			condition[2] = values[1].trim();
		}

		return condition;
	}


	public static void createTable(RandomAccessFile table,String tableName, String[] columnNames) throws ParserConfigurationException {
		
		File file = new File("data/catalog/tables.xml");
		
		
		try{
			DocumentBuilderFactory docConstraints = DocumentBuilderFactory.newInstance();
			
			DocumentBuilder docConstraintsBuilder = docConstraints.newDocumentBuilder();
			
			Document doc = docConstraintsBuilder.parse(file);
			Element rootElement = doc.getDocumentElement();
//			Element rootElement = doc.createElement("davisbase_tables");
//			doc.appendChild(rootElement);
			
			
			// configure new blank page
			table.setLength(PAGESIZE);
			table.seek(0);
			table.writeByte(0x0D);
			table.seek(2);
			table.writeShort(PAGESIZE);
			table.writeInt(-1);
			table.close();
			
			//update Davisbase_tables
			RandomAccessFile davisbaseTables=new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int num_of_pages=(int) (davisbaseTables.length()/PAGESIZE);
			int page=0;
			
			Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
			for(int i=0;i<num_of_pages;i++)
			{
				davisbaseTables.seek((i*PAGESIZE)+4);
				int filePointer=davisbaseTables.readInt();
				if(filePointer==-1){
					page=i;
					davisbaseTables.seek(i*PAGESIZE+1);
					int noOfCells = davisbaseTables.readByte();
					short[] cellLocations = new short[noOfCells];
					davisbaseTables.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = davisbaseTables.readShort();
					}
					recordCells = getStuff.getRecords(davisbaseTables, cellLocations,i);
				}
			}
			davisbaseTables.close();
			Set<Integer> rowIds=recordCells.keySet();
			Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
			Integer rows[]=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			int key=rows[rows.length-1]+1;
			
			String[] values = {String.valueOf(key),tableName.trim(),"8","10"};
			Insert("davisbase_tables", values);
			
			//Update Davisbase_columns
			RandomAccessFile davisbaseColumns=new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			num_of_pages=(int) (davisbaseColumns.length()/PAGESIZE);
			page=0;
			
			recordCells = new LinkedHashMap<Integer, Cell>();
			for(int i=0;i<num_of_pages;i++)
			{
				davisbaseColumns.seek((i*PAGESIZE)+4);
				int filePointer=davisbaseColumns.readInt();
				if(filePointer==-1){
					page=i;
					davisbaseColumns.seek(i*PAGESIZE+1);
					int noOfCells = davisbaseColumns.readByte();
					short[] cellLocations = new short[noOfCells];
					davisbaseColumns.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = davisbaseColumns.readShort();
					}
					recordCells = getStuff.getRecords(davisbaseColumns, cellLocations,i);
				}
			}
			rowIds=recordCells.keySet();
			sortedRowIds = new TreeSet<Integer>(rowIds);
			rows=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			key=rows[rows.length-1];
			
			for(int i = 0; i < columnNames.length; i++){
				key = key + 1;
				
				String[] coltemp = columnNames[i].split(" ");
				String isNullable="YES";
				
				if(coltemp.length==3)
				{
					if(coltemp[2].equalsIgnoreCase("UNIQUE"))
					{
						
						Element tablename = doc.createElement("table_name");
						//tablename.appendChild(doc.createTextNode(tableName));
						rootElement.appendChild(tablename);
						
						Element tableid = doc.createElement("table_id");
						tableid.appendChild(doc.createTextNode(tableName));
						tablename.appendChild(tableid);
						
//						Attr attr = doc.createAttribute("id");
//						attr.setValue("1");
//						staff.setAttributeNode(attr);
						
						Element colname = doc.createElement("column_name");
						colname.appendChild(doc.createTextNode(Integer.toString(i)));
						tablename.appendChild(colname);
						
						Element constraint = doc.createElement("constraint_name");
						constraint.appendChild(doc.createTextNode("unique"));
						tablename.appendChild(constraint);
						
						Element value = doc.createElement("value");
						value.appendChild(doc.createTextNode("YES"));
						tablename.appendChild(value);
					}
					if(coltemp[2].equalsIgnoreCase("AUTOINCREMENT"))
					{
						
						Element tablename = doc.createElement("table_name");
						//tablename.appendChild(doc.createTextNode(tableName));
						rootElement.appendChild(tablename);
						
						Element tableid = doc.createElement("table_id");
						tableid.appendChild(doc.createTextNode(tableName));
						tablename.appendChild(tableid);
						
//						Attr attr = doc.createAttribute("id");
//						attr.setValue("1");
//						staff.setAttributeNode(attr);
						
						Element colname = doc.createElement("column_name");
						colname.appendChild(doc.createTextNode(Integer.toString(i)));
						tablename.appendChild(colname);
						
						Element constraint = doc.createElement("constraint_name");
						constraint.appendChild(doc.createTextNode("autoincrement"));
						tablename.appendChild(constraint);
						
						Element value = doc.createElement("value");
						value.appendChild(doc.createTextNode("YES"));
						tablename.appendChild(value);
					}
				}
				if(coltemp.length==4)
				{
					if(coltemp[2].equalsIgnoreCase("DEFAULT"))
					{
						
						Element tablename = doc.createElement("table_name");
						//tablename.appendChild(doc.createTextNode(tableName));
						rootElement.appendChild(tablename);
						
						Element tableid = doc.createElement("table_id");
						tableid.appendChild(doc.createTextNode(tableName));
						tablename.appendChild(tableid);
						
//						Attr attr = doc.createAttribute("id");
//						attr.setValue("1");
//						staff.setAttributeNode(attr);
						
						Element colname = doc.createElement("column_name");
						colname.appendChild(doc.createTextNode(Integer.toString(i)));
						tablename.appendChild(colname);
						
						Element constraint = doc.createElement("constraint_name");
						constraint.appendChild(doc.createTextNode("default"));
						tablename.appendChild(constraint);
						
						Element value = doc.createElement("value");
						value.appendChild(doc.createTextNode(coltemp[3]));
						tablename.appendChild(value);
					}
					if(coltemp[2].equalsIgnoreCase("NOT") && coltemp[3].equalsIgnoreCase("NULL"))
					{
						isNullable="NO";	
					}
					if(coltemp[2].equalsIgnoreCase("PRIMARY") && coltemp[3].equalsIgnoreCase("KEY"))
					{
						isNullable="NO";
					}
					
				}
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer tr = transformerFactory.newTransformer();
				tr.transform(new DOMSource(doc), 
	                    new StreamResult(new FileOutputStream(file)));
				
				String colName = coltemp[0];
				String dataType = coltemp[1].toUpperCase();
				String ordinalPosition = String.valueOf(i+1);
				String[] val = {String.valueOf(key), tableName, colName, dataType, ordinalPosition, isNullable};
				Insert("davisbase_columns", val);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}


	public static void dropTable(String tableName) {
		
		try
		{
			RandomAccessFile davisbaseTables=new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			updateMetaOffset(davisbaseTables,"davisbase_tables",tableName);
			
			
			RandomAccessFile davisbaseColumns=new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			updateMetaOffset(davisbaseColumns,"davisbase_columns",tableName);
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
	public static void updateMetaOffset(RandomAccessFile davisbaseTables,String metaTable,String tableName) throws IOException
	{
		int num_of_pages = (int) (davisbaseTables.length() / PAGESIZE);

		Map<Integer, String> colNames = getStuff.getColumnNames(metaTable);
		
		for (int i = 0; i < num_of_pages; i++) {
			davisbaseTables.seek(PAGESIZE * i);
			byte pageType = davisbaseTables.readByte();
			if (pageType == 0x0D) {

				int noOfCells = davisbaseTables.readByte();
				short[] cellLocations = new short[noOfCells];
				davisbaseTables.seek((PAGESIZE * i) + 8);
				for (int location = 0; location < noOfCells; location++) {
					cellLocations[location] = davisbaseTables.readShort();
				}
				Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
				recordCells = getStuff.getRecords(davisbaseTables, cellLocations,i);
				
				
				String[] condition={"table_name","<>",tableName};
				String[] columnNames={"*"};
				
				Map<Integer,Cell> filteredRecs=filterRecordsByData(colNames, recordCells, columnNames, condition);
				short[] offsets=new short[filteredRecs.size()];
				int l=0;
				for (Map.Entry<Integer, Cell> entry : filteredRecs.entrySet()){
					Cell cell=entry.getValue();
					offsets[l]=cell.get_Location();
					davisbaseTables.seek(i*PAGESIZE+8+(2*l));
					davisbaseTables.writeShort(offsets[l]);
					l++;
				}
				
				davisbaseTables.seek((PAGESIZE * i)+1);
				davisbaseTables.writeByte(offsets.length);
				davisbaseTables.writeShort(offsets[offsets.length-1]);
				//davisbaseTables.close();
			}
		}
	
	}

	public static void delete(String tableName, String[] cond) throws IOException {
		
		
		String path="data/userdata/"+tableName+".tbl";
		if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
			path="data/catalog/"+tableName+".tbl";
		
		
		try {
			RandomAccessFile table=new RandomAccessFile(path,"rw");
			
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Cell> columnsMeta = getStuff.getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = getStuff.getDataType(columnsMeta);
			String[] isNullable = getStuff.isNullable(columnsMeta);
			Map<Integer, String> colNames = getStuff.getColumnNames(tableName);
			
			condition = new String[0];
			
			//get page number on which data exist
			int pageNo= getStuff.getPageNo(tableName,Integer.parseInt(cond[2]));
			
			//check for duplicate value
			Map<Integer, Cell> data = getStuff.getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(cond[2]))) {
				table.seek((PAGESIZE * pageNo) + 1);
				int noOfCells = table.readByte();
				short[] cellLocations = new short[noOfCells];
				table.seek((PAGESIZE * pageNo) + 8);
				for (int location = 0; location < noOfCells; location++) {
					cellLocations[location] = table.readShort();
				}
				Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
				recordCells = getStuff.getRecords(table, cellLocations,pageNo);
				
				String[] condition1={cond[0],"<>",cond[2]};
				String[] columnNames1={"*"};
				
				Map<Integer,Cell> filteredRecs=filterRecordsByData(colNames, recordCells, columnNames, condition1);
				short[] offsets=new short[filteredRecs.size()];
				int l=0;
				for (Map.Entry<Integer, Cell> entry : filteredRecs.entrySet()){
					Cell cell=entry.getValue();
					offsets[l]=cell.get_Location();
					table.seek(pageNo*PAGESIZE+8+(2*l));
					table.writeShort(offsets[l]);
					l++;
				}
				
				table.seek((PAGESIZE*pageNo)+1);
				table.writeByte(offsets.length);
				table.writeShort(offsets[offsets.length-1]);
				table.close();
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
public static void Update(String tableName, String[] set, String[] cond) {
		
	String path="data/userdata/"+tableName+".tbl";
	if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
		path="data/catalog/"+tableName+".tbl";
	
	
	try {
		RandomAccessFile file=new RandomAccessFile(path,"rw");
		
		String condition[] = { "table_name", "=", tableName};
		String columnNames[] = { "*" };
		Map<Integer, Cell> columnsMeta = getStuff.getColumnsMeta(tableName, columnNames, condition);
		String[] dataType = getStuff.getDataType(columnsMeta);
		String[] isNullable = getStuff.isNullable(columnsMeta);
		Map<Integer, String> colNames = getStuff.getColumnNames(tableName);
		
		//ordinal position
		int k = -1;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(set[0])) {
				k = entry.getKey();
			}
		}
		
		if(cond.length>0){
		int key=Integer.parseInt(cond[2]);
		condition = new String[0];
		
		//get page number on which data exist
		int pageno= getStuff.getPageNo(tableName,Integer.parseInt(cond[2]));
		
		//check for duplicate value
		Map<Integer, Cell> data = getStuff.getData(tableName, columnNames, condition);
		if (data.containsKey(Integer.parseInt(cond[2]))) {
				
				try {
					file.seek((pageno)*PAGESIZE+1);
					int records = file.read();
					short[] offsetLocations = new short[records];
					//TreeMap<Integer, Short> offsets = new TreeMap<Integer, Short>();
					
					
					for(int j=0;j<records;j++){
						file.seek((pageno)*PAGESIZE+8+2*j);
						offsetLocations[j]=file.readShort();
						file.seek(offsetLocations[j]+2);
						//int pay_size = file.readShort();
						int ky=file.readInt();
							if(key==ky){
								int no=file.read();
								byte[] sc = new byte[no];
								file.read(sc);
								int seek_positions=0;
								for(int i=0;i<k-2;i++){
									seek_positions+= getStuff.dataLength(sc[i]);
								}
								file.seek(offsetLocations[j]+6+no+1+seek_positions);
								
								
								byte sc_update = sc[k-2];
								switch (sc_update){
								
								case 0x00:	file.write(Integer.parseInt(set[2]));
											sc[k-2]=0x04;
											break;
								case 0x01:	file.writeShort(Integer.parseInt(set[2]));
											sc[k-2]=0x05;
											break;
								case 0x02:	file.writeInt(Integer.parseInt(set[2]));
											sc[k-2]=0x06;
											break;
								case 0x03:	file.writeDouble(Double.parseDouble(set[2]));
											sc[k-2]=0x09;
											break;
								case 0x04:	file.write(Integer.parseInt(set[2]));
											break;
								case 0x05:	file.writeShort(Integer.parseInt(set[2]));
											break;
								case 0x06:	file.writeInt(Integer.parseInt(set[2]));
											break;
								case 0x07:	file.writeLong(Long.parseLong(set[2]));
											break;
								
								case 0x08: 	file.writeFloat(Float.parseFloat(set[2]));
											break;
											
								case 0x09:	file.writeDouble(Double.parseDouble(set[2]));
											break;
											
								}
								
								file.seek(offsetLocations[j]+7);
								file.write(sc);

							}
					}
				
				}catch (Exception e) {
					e.printStackTrace(System.out);
				}
			}
		}
			else{
				
					try {
						int no_of_pages = (int) (file.length()/PAGESIZE);
						for(int l=0;l<no_of_pages;l++){
						file.seek(l*PAGESIZE);
						byte pageType=file.readByte();
						if(pageType==0x0D){
						
						file.seek((l)*PAGESIZE+1);
						int records = file.read();
						short[] offsetLocations = new short[records];
						
						for(int j=0;j<records;j++){
							file.seek((l)*PAGESIZE+8+2*j);
							offsetLocations[j]=file.readShort();
							file.seek(offsetLocations[j]+6);
							//int pay_size = file.readShort();
								
								
									int no=file.read();
									byte[] sc = new byte[no];
									file.read(sc);
									int seek_positions=0;
									for(int i=0;i<k-2;i++){
										seek_positions+=getStuff.dataLength(sc[i]);
									}
									file.seek(offsetLocations[j]+6+no+1+seek_positions);
									
									
									byte sc_update = sc[k-2];
									switch (sc_update){
									
									case 0x00:	file.write(Integer.parseInt(set[2]));
												sc[k-2]=0x04;
												break;
									case 0x01:	file.writeShort(Integer.parseInt(set[2]));
												sc[k-2]=0x05;
												break;
									case 0x02:	file.writeInt(Integer.parseInt(set[2]));
												sc[k-2]=0x06;
												break;
									case 0x03:	file.writeDouble(Double.parseDouble(set[2]));
												sc[k-2]=0x09;
												break;
									case 0x04:	file.write(Integer.parseInt(set[2]));
												break;
									case 0x05:	file.writeShort(Integer.parseInt(set[2]));
												break;
									case 0x06:	file.writeInt(Integer.parseInt(set[2]));
												break;
									case 0x07:	file.writeLong(Long.parseLong(set[2]));
												break;
									
									case 0x08: 	file.writeFloat(Float.parseFloat(set[2]));
												break;
												
									case 0x09:	file.writeDouble(Double.parseDouble(set[2]));
												break;
												
									}
									
									file.seek(offsetLocations[j]+7);
									file.write(sc);

								}
							}
						}
					}catch (Exception e) {
						e.printStackTrace(System.out);
					}
			}
			}catch (Exception e) {
				e.printStackTrace(System.out);
			}
	}
}
