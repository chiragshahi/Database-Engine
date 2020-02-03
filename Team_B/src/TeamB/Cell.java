package shruti;

public class Cell {
	
	int pageNumber;
	short payLoadSize;
	int rowId;
	Payload payload;
	short location;
	
	public short get_Location() {
		return location;
	}
	public void set_Location(short location) {
		this.location = location;
	}
	public int get_PageNumber() {
		return pageNumber;
	}
	public void set_PageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	
	public short get_PayLoadSize() {
		return payLoadSize;
	}
	public void set_PayLoadSize(short payLoadSize) {
		this.payLoadSize = payLoadSize;
	}
	public int get_RowId() {
		return rowId;
	}
	public void set_RowId(int rowId) {
		this.rowId = rowId;
	}
	public Payload get_Payload() {
		return payload;
	}
	public void set_Payload(Payload payload) {
		this.payload = payload;
	}
	
	

}
