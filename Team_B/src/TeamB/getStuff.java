package shruti;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.Optional;

public class getStuff implements Constants {

	public static int getParent(RandomAccessFile table, int page) {

		try {
			int numpages = (int) (table.length() / PAGESIZE);
			for (int i = 0; i < numpages; i++) {

				table.seek(i * PAGESIZE);
				byte pageType = table.readByte();

				if (pageType == 0x05) {
					table.seek(i * PAGESIZE + 4);
					int p = table.readInt();
					if (page == p)
						return i + 1;

					table.seek(i * PAGESIZE + 1);
					int numrecords = table.read();
					short[] offsets = new short[numrecords];

					// insertFile.read(offsets);
					for (int j = 0; j < numrecords; j++) {
						table.seek(i * PAGESIZE + 8 + 2 * j);
						offsets[i] = table.readShort();
						table.seek(offsets[i]);
						if (page == table.readInt())
							return j + 1;

					}

				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public static int getPayloadSize(String tableName, String[] values, byte[] plDataType, String[] dataType) {

		int size = 1 + dataType.length - 1;
		for (int i = 0; i < values.length-1; i++) {
			plDataType[i] = getDataTypeCode(values[i + 1], dataType[i + 1]);
			size=size+dataLength(plDataType[i]);
		}

		return size;
	}

	private static byte getDataTypeCode(String value, String dataType) {
		if (value.equals("null")) {
			switch (dataType) {
			case "TINYINT":
				return 0x00;
			case "SMALLINT":
				return 0x01;
			case "INT":
				return 0x02;
			case "BIGINT":
				return 0x03;
			case "REAL":
				return 0x02;
			case "DOUBLE":
				return 0x03;
			case "DATETIME":
				return 0x03;
			case "DATE":
				return 0x03;
			case "TEXT":
				return 0x03;
			default:
				return 0x00;
			}
		} else {
			switch (dataType) {
			case "TINYINT":
				return 0x04;
			case "SMALLINT":
				return 0x05;
			case "INT":
				return 0x06;
			case "BIGINT":
				return 0x07;
			case "REAL":
				return 0x08;
			case "DOUBLE":
				return 0x09;
			case "DATETIME":
				return 0x0A;
			case "DATE":
				return 0x0B;
			case "TEXT":
				return (byte) (value.length() + 0x0C);
			default:
				return 0x00;
			}
		}
	}

	public static short dataLength(byte codes) {
		switch (codes) {
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			return (short) (codes - 0x0C);
		}
	}
	
	
	public static int getPageNo(String tableName, int key) {
		try {

			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			
			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
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
					recordCells = getRecords(table, cellLocations,i);
					
					Set<Integer> rowIds=recordCells.keySet();
					
					Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
					
					Integer rows[]=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
					
					//last page
					table.seek((PAGESIZE * i)+4);
					int filePointer = table.readInt();
					
					if(rowIds.size()==0)
						return 0;
					if(rows[0] <= key && key <= rows[rows.length - 1])
						return i;
					else if(filePointer== -1 && rows[rows.length-1]<key)
					    return i;
				}
			}

		}
			
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return -1;
	}
	


	public static Map<Integer, Cell> getData(String tableName, String[] columnNames, String[] condition) {
		try {

			
			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			
			Map<Integer,Page> pageInfo=new LinkedHashMap<Integer, Page>();
			
			
			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					
					Page page=new Page();
					page.setPageNo(i);
					page.setPageType(pageType);
					
					
					
					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getRecords(table, cellLocations,i);
					
					page.setRecords(recordCells);
					pageInfo.put(i, page);
					
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Cell> filteredRecords = Operations.filterRecords(colNames, records, columnNames, condition);
				// printTable(colNames, filteredRecords);
				return filteredRecords;
			} else {
				//printTable(colNames, records);
				return records;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	public static String[] getDataType(Map<Integer, Cell> columnsMeta) {
		int count = 0;
		String[] dataType = new String[columnsMeta.size()];
		for (Map.Entry<Integer, Cell> entry : columnsMeta.entrySet()) {

			Cell cell = entry.getValue();
			Payload payload = cell.get_Payload();
			String[] data = payload.get_Data();
			dataType[count] = data[2];
			count++;
		}
		return dataType;
	}

	public static String[] isNullable(Map<Integer, Cell> columnsMeta) {
		int count = 0;
		String[] nullable = new String[columnsMeta.size()];
		for (Map.Entry<Integer, Cell> entry : columnsMeta.entrySet()) {

			Cell cell = entry.getValue();
			Payload payload = cell.get_Payload();
			String[] data = payload.get_Data();
			nullable[count] = data[4];
			count++;
		}
		return nullable;
	}

	public static Map<Integer, Cell> getColumnsMeta(String tableName, String[] columnNames, String[] condition) {

		try {
			
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			Map<Integer, String> colNames = getColumnNames("davisbase_columns");
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
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
					recordCells = getRecords(table, cellLocations,i);
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Cell> filteredRecords = Operations.filterRecords(colNames, records, columnNames, condition);
				// printTable(colNames, filteredRecords);
				return filteredRecords;
			} else {
				return records;
				// printTable(colNames, records);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;

	}

	public static void printTable(Map<Integer, String> colNames, Map<Integer, Cell> records) {
		String colString = "";
		String recString = "";
		ArrayList<String> recList = new ArrayList<String>();

		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {

			String colName = entry.getValue();
			colString += colName + " | ";
		}
		System.out.println(colString);
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {

			Cell cell = entry.getValue();
			recString += cell.get_RowId();
			String data[] = cell.get_Payload().get_Data();
			for (String dataS : data) {
				recString = recString + " | " + dataS;
			}
			System.out.println(recString);
			recString = "";
		}

	}

	public static Map<Integer, String> getColumnNames(String tableName) {
		Map<Integer, String> columns = new LinkedHashMap<Integer, String>();
		try {
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			for (int i = 0; i < noOfPages; i++) {
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
					recordCells = getRecords(table, cellLocations,i);

					for (Map.Entry<Integer, Cell> entry : recordCells.entrySet()) {

						Cell cell = entry.getValue();
						// System.out.println("Key : " + entry.getKey() + "
						// Value : " + entry.getValue());
						Payload payload = cell.get_Payload();
						String[] data = payload.get_Data();
						if (data[0].equalsIgnoreCase(tableName)) {
							columns.put(Integer.parseInt(data[3]), data[1]);
						}

					}

					//System.out.println(recordCells);
				}

			}
		} catch (Exception e) {

			e.printStackTrace();
		}

		return columns;

	}

	public static Map<Integer, Cell> getRecords(RandomAccessFile table, short[] cellLocations, int pageNo) {

		Map<Integer, Cell> cells = new LinkedHashMap<Integer, Cell>();
		for (int position = 0; position < cellLocations.length; position++) {
			try {
				Cell cell = new Cell();
				cell.set_PageNumber(pageNo);
				cell.set_Location(cellLocations[position]);
				
				table.seek(cellLocations[position]);

				short payLoadSize = table.readShort();
				cell.set_PayLoadSize(payLoadSize);

				int rowId = table.readInt();
				cell.set_RowId(rowId);

				Payload payload = new Payload();
				byte num_cols = table.readByte();
				payload.set_NumberOfColumns(num_cols);

				byte[] dataType = new byte[num_cols];
				int colsRead = table.read(dataType);
				payload.set_DataType(dataType);

				String data[] = new String[num_cols];
				payload.set_Data(data);

				for (int i = 0; i < num_cols; i++) {
					switch (dataType[i]) {
					case 0x00:
						data[i] = Integer.toString(table.readByte());
						data[i] = "null";
						break;

					case 0x01:
						data[i] = Integer.toString(table.readShort());
						data[i] = "null";
						break;

					case 0x02:
						data[i] = Integer.toString(table.readInt());
						data[i] = "null";
						break;

					case 0x03:
						data[i] = Long.toString(table.readLong());
						data[i] = "null";
						break;

					case 0x04:
						data[i] = Integer.toString(table.readByte());
						break;

					case 0x05:
						data[i] = Integer.toString(table.readShort());
						break;

					case 0x06:
						data[i] = Integer.toString(table.readInt());
						break;

					case 0x07:
						data[i] = Long.toString(table.readLong());
						break;

					case 0x08:
						data[i] = String.valueOf(table.readFloat());
						break;

					case 0x09:
						data[i] = String.valueOf(table.readDouble());
						break;

					case 0x0A:
						long tmp = table.readLong();
						Date dateTime = new Date(tmp);
						// data[i] = formater.format(dateTime);
						break;

					case 0x0B:
						long tmp1 = table.readLong();
						Date date = new Date(tmp1);
						// data[i] = formater.format(date).substring(0,10);
						break;

					default:
						int len = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[len];
						for (int j = 0; j < len; j++)
							bytes[j] = table.readByte();
						data[i] = new String(bytes);
						break;
					}

				}

				cell.set_Payload(payload);
				cells.put(rowId, cell);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return cells;
	}

}