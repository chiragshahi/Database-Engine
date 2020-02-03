package shruti;

public class Payload {
	
	
	byte numberOfColumns;
	byte[] dataType;
	String[] data;
	
	
	public byte get_NumberOfColumns() {
		return numberOfColumns;
	}
	public void set_NumberOfColumns(byte numberOfColumns) {
		this.numberOfColumns = numberOfColumns;
	}
	public byte[] get_DataType() {
		return dataType;
	}
	public void set_DataType(byte[] dataType) {
		this.dataType = dataType;
	}
	public String[] get_Data() {
		return data;
	}
	public void set_Data(String[] data) {
		this.data = data;
	}
	
	

}
