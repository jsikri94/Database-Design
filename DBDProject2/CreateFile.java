	import java.io.File;
	import java.io.FileNotFoundException;
	import java.io.IOException;
	import java.io.RandomAccessFile;
	import java.time.*;
	import java.util.ArrayList;
	import java.util.HashMap;
	import java.util.Map;

	public class CreateFile extends RandomAccessFile {

		/*
		 * File object for creating RandomAccessFile object
		 */
		File file;
		/*
		 * PageSize
		 */
		int pageSize;
		/*
		 * PageType
		 */
		int pageType;
		/*
		 * Total number of records in a table
		 */
		int numberOfRecords;
		/*
		 * Total number of records in a single apge of a table
		 */
		int numberOfRecordsPage;
		/*
		 * Total number of records in root
		 */
		int rootNumberOfRecords;
		
		/*
		 * Position pointer for root page
		 */
		int rootPos;

		/*
		 * Position pointer for davisbase_tables page
		 */
		int rfTblPos;
		
		/*
		 * Position pointer for davisbase_columns page
		 */
		int rfColPos;
		
		/*
		 * Position pointer for user table page
		 */
		int pos;
		/*
		 * Position pointer for start of array of record start positions in file header
		 */
		int recordStartPostion = 8;
		/*
		 * Position pointer for start of array of record start positions in file header of root page
		 */
		int rootRecordStartPostion = 8;

		int rowId;
		
		/*
		 * Record Length for a table
		 */
		int recordLength;
		
		/*
		 * Number of columns in a table
		 */
		int numberOfColumns;
		
		/*
		 * Number of pages in a table
		 */
		int countPages = 1;

		/*
		datatype of columns of a table
		*/
		ArrayList<String> cols_dt = new ArrayList<>();
		
		//constructor
		public CreateFile(File file, String mode, int pageSize) throws FileNotFoundException {
			super(file, mode);
			this.file = file;
			this.pageSize = pageSize;
			pos = pageSize;
			rootPos = pageSize;
			rfTblPos = pageSize;
			rfColPos = pageSize;
		}
		/*
		inserting data into the davisbase_tables
		*/
		public void insertIntoDavisBaseTable(String tableName) throws IOException {
		
		recordLength = 2+4+1+2+20;
		seek(0);
		writeByte(13);
		//Set Number of cells
		seek(1);
		numberOfRecords += 1;
		writeByte(numberOfRecords);
		rfTblPos = rfTblPos - recordLength;
		//Set start of cell content area
		seek(2);
		writeShort(rfTblPos);

		seek(4);
		writeInt(-1);
		
		//Set Address for each record
		seek(recordStartPostion);
		writeShort(rfTblPos);
		recordStartPostion += 2;//because each is 2 bytes long
		
		seek(rfTblPos);
		writeShort(23);
		
		seek(rfTblPos+2);
		writeInt(++rowId);	
		
		numberOfColumns = 2;

		seek(rfTblPos+6);
		writeByte(numberOfColumns);

		seek(rfTblPos+7);
		writeByte(6);

		seek(rfTblPos+8);
		writeByte(20);

		seek(rfTblPos+9);
		writeBytes(tableName);
	}
	public boolean calcFreeSpace(int rL, int pos, int n, int ct){
		boolean free = false;
		if ((pos - (pageSize*(ct-1)+(n*2)+7))-2>rL){
			free = true;
		}

		return free;
	}
	public void insertIntoDavisBaseColumn(String tableName, String columnName, String data_type, int ordinal_position, String is_nullable) throws IOException {
		
		recordLength = 2+4+1+6+20+20+20+4+4;

		if(calcFreeSpace(recordLength, rfColPos, numberOfRecordsPage, countPages)){
			//add a new record to same page if there is free space available
			seek(pageSize*(countPages-1)+0);
			writeByte(13);

			seek(pageSize*(countPages-1)+1);
			//System.out.println(numberOfRecords);
			numberOfRecords += 1;
			numberOfRecordsPage +=1;
			writeByte(numberOfRecordsPage);

			rfColPos = rfColPos - recordLength;
			//System.out.println(rfColPos);
			
			//Set Address of last record inserted
			seek(pageSize*(countPages-1)+2);
			writeShort(rfColPos);
			
			seek(pageSize*(countPages-1)+recordStartPostion);
			writeShort(rfColPos);
			//System.out.println("writing if at "+(pageSize*(countPages-1)+recordStartPostion)+" value: "+rfColPos);
			recordStartPostion += 2;
			
			seek(rfColPos);
			writeShort(75);
			seek(rfColPos+2);
			writeInt(++rowId);

			numberOfColumns = 6;

			seek(rfColPos+6);
			writeByte(numberOfColumns);
			seek(rfColPos+7);
			writeByte(6);
			seek(rfColPos+8);
			writeByte(20);
			seek(rfColPos+9);
			writeByte(20);
			seek(rfColPos+10);
			writeByte(20);
			seek(rfColPos+11);
			writeByte(4);
			seek(rfColPos+12);
			writeByte(20);

			seek(rfColPos+13);
			writeBytes(tableName);
			seek(rfColPos+33);
			writeBytes(columnName);
			seek(rfColPos+53);
			writeBytes(data_type);
			seek(rfColPos+73);
			writeInt(ordinal_position);
			
			if(is_nullable.equalsIgnoreCase("not_null")){
				seek(rfColPos+77);
				writeBytes("NO");
			}else{
				seek(rfColPos+77);
				writeBytes("YES");
			}

			seek(pageSize*(countPages-1)+4);
			writeInt(-1);
			//System.out.println("IF : "+countPages+" "+numberOfRecordsPage+" "+recordStartPostion);
		}else{
			//create a new page and also a root page(aaaaaah)

			setLength(pageSize*(countPages+1));
			seek(pageSize*(countPages-1)+4);
			writeInt(countPages+1);	
			countPages++;
			//System.out.println(" countPages in else "+countPages);
			numberOfRecordsPage = 0;
			
			rfColPos = pageSize*countPages;
			//System.out.println(rfColPos);
			recordStartPostion =8;

			seek(pageSize*(countPages-1));
			writeByte(13);

			seek(pageSize*(countPages-1)+1);
			//System.out.println(numberOfRecords);
			numberOfRecords += 1;
			numberOfRecordsPage +=1;
			writeByte(numberOfRecordsPage);

			rfColPos = rfColPos - recordLength;
			//System.out.println(rfColPos);
			//System.out.println(rfColPos);
			
			//Set Address of last record inserted
			seek(pageSize*(countPages-1)+2);
			writeShort(rfColPos);
			
			seek(pageSize*(countPages-1)+recordStartPostion);
			writeShort(rfColPos);
			//System.out.println("writing else at "+(pageSize*(countPages-1)+recordStartPostion)+" value: "+rfColPos);
			recordStartPostion += 2;
			
			seek(rfColPos);
			writeShort(75);
			seek(rfColPos+2);
			writeInt(++rowId);

			numberOfColumns = 6;

			seek(rfColPos+6);
			writeByte(numberOfColumns);
			seek(rfColPos+7);
			writeByte(6);
			seek(rfColPos+8);
			writeByte(20);
			seek(rfColPos+9);
			writeByte(20);
			seek(rfColPos+10);
			writeByte(20);
			seek(rfColPos+11);
			writeByte(4);
			seek(rfColPos+12);
			writeByte(20);

			seek(rfColPos+13);
			writeBytes(tableName);
			seek(rfColPos+33);
			writeBytes(columnName);
			seek(rfColPos+53);
			writeBytes(data_type);
			seek(rfColPos+73);
			writeInt(ordinal_position);
			if(is_nullable.equalsIgnoreCase("not_null")){
				seek(rfColPos+77);
				writeBytes("NO");
			}else{
				seek(rfColPos+77);
				writeBytes("YES");
			}
			//System.out.println("ELSE : "+countPages+" "+numberOfRecordsPage+" "+recordStartPostion);

			//creating root page
			if(countPages==2){
				//System.out.println("adding root page");
				setLength(pageSize*3);
				seek(1024);
				writeByte(5);
				rootNumberOfRecords++;
				seek(1025);
				writeByte(rootNumberOfRecords);
			}

			//inserting values into root page
			rootPos = 1536;
			rootPos = rootPos - 8;
			seek(rootPos);
			writeInt(countPages-1);
			seek(rootPos+4);
			writeInt(rowId-1);
			seek(1024+rootRecordStartPostion);
			writeShort(rootPos);
			rootRecordStartPostion +=2;
			
		}		
	}

	public int calculateNumberOfRecords(int countPages) throws IOException {
		int numOfRecords = 0;
		for(int i=1;i<=countPages;i++){
			if(i!=3){
				seek(pageSize*(i-1)+1);
				numOfRecords +=readByte();
			}
		}
		return numOfRecords;
	}
	public int calculateNumberOfRecordsPage(int countPages) throws IOException {
		int numOfRecordsP = 0;
		seek(pageSize*(countPages-1)+1);
		numOfRecordsP = readByte();
		return numOfRecordsP;
	}


	public int[] calculateNumberOfColumnsAndRecordLength(String tableName) throws IOException {

		int numberOfColumns = 0;
		int recordLength = 0;
		int r_start,n , position;
		String referenceTableLine, dt_string;
		//System.out.println(countPages);
		for(int i=1;i<=countPages;i++){
			//System.out.println(tableName+" "+i);
			if(i!=3){
				seek(pageSize*(i-1)+1);
				n = readByte();
				r_start = pageSize*(i-1)+8;
				//System.out.println(tableName+" "+i+" "+r_start);
				for(int k=0;k<n;k++){
					//go through all records on current page
					//find the table name
					
					seek(r_start);
					position = readShort();
					seek(position+13);

					referenceTableLine = readLine().substring(0, 20).trim();
					if(referenceTableLine.equalsIgnoreCase(tableName)){
						numberOfColumns++;
						seek(position+53);
						dt_string = readLine().substring(0,20).trim();
						recordLength += getRecordLengthForDataType(dt_string);
					}
					r_start +=2;
				}				
			}
		}
		int values[] = new int[2];
		values[0] = numberOfColumns;
		values[1] = recordLength;
		return values;
		
	}

	public ArrayList<String> getColsDt(String tableName) throws IOException {

		ArrayList<String> col = new ArrayList<>();
		int numberOfColumns = 0;
		int recordLength = 0;
		int r_start,n , position;
		String referenceTableLine, dt_string;
		for(int i=1;i<=countPages;i++){
			if(i!=3){
				seek(pageSize*(i-1)+1);
				n = readByte();
				r_start = pageSize*(i-1)+8;

				for(int k=0;k<n;k++){
					//go through all records on current page
					//find the table name
					
					seek(r_start);
					position = readShort();
					seek(position+13);

					referenceTableLine = readLine().substring(0, 20).trim();
					if(referenceTableLine.equalsIgnoreCase(tableName)){
						seek(position+53);
						dt_string = readLine().substring(0,20).trim();
						col.add(dt_string);
						//System.out.println("adding "+dt_string+" to cols_dt");
					}
					r_start +=2;
				}				
			}
		}
		return col;
		
	}


	public void processShowTableQuery() throws IOException {
		//ArrayList<String> listOfTables = new ArrayList<>();
		int startPos = 8;
		seek(startPos);
		int start = readShort();
		seek(1);
		int count = readByte();

		System.out.println("+-----------------------+");
		System.out.println("|\tTable_name\t|");
		System.out.println("+-----------------------+");
		while(count != 0) {
			seek(start+9);
			System.out.println("|"+readLine().substring(0,20).trim()+"\t|");
			startPos += 2;
			seek(startPos);
			start = readShort();
			count--;
		}
		System.out.println("+-----------------------+");
		System.out.println("\n");
	}

	public boolean checkIfTableAlreadyExists(String tableName) throws IOException {
		seek(1);
		int numberOfRecords = readByte();
		
		seek(2);
		int rfTblPosition = readShort() + 9; // this position will give us the table name
		seek(rfTblPosition);
		String rfTblReferenceTableLine = readLine().substring(0, 20);
		int m = 1;
		int i;
		for (i = 0; i < (numberOfRecords-2); i++) {
			if(!rfTblReferenceTableLine.equalsIgnoreCase(tableName)) { 
				rfTblPosition = rfTblPosition + 29*(m);
				seek(rfTblPosition);
				rfTblReferenceTableLine = readLine().substring(0, 20);
			}
			else 
				break;
		}
		
		if(i == (numberOfRecords-2))
			return false;
		else
			return true;
	}

	public void setPageType(int pageType) throws IOException {
		this.pageType = pageType;
		seek(0);
		writeByte(pageType);
	}

	public void calculateRecordLength(String columnDetails) {
		columnDetails = columnDetails.substring(0, columnDetails.length()-1);
		String columns[] = columnDetails.split(",");
		numberOfColumns = columns.length;
		
		for (int i = 0; i < columns.length; i++) {
			String temp1[] = columns[i].split(" ");
			recordLength += getRecordLengthForDataType(temp1[1]);
		}
		//System.out.println("Record Length: " + recordLength);
	}

	public int getRootRecords() throws IOException{
		int rr;
		seek(1025);
		rr = readByte();

		return rr;
	}

	private int getRecordLengthForDataType(String data_type) {
		int record_size = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            record_size=record_size+4;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            record_size=record_size+1;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            record_size=record_size+2;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            record_size=record_size+4;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            record_size=record_size+20;
        }
		return record_size;
	}

	public int getSerialCode(String data_type) {
		int serialCode = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            serialCode=0x06;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            serialCode=0x04;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            serialCode=0x05;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            serialCode=0x07;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            serialCode=0x08;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            serialCode=0x09;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            serialCode=0x0A;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            serialCode=0x0B;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            serialCode=0x0C;
        }		
		return serialCode;
	}

	public void insertUserColumsEntry(String tableName, String columnDetails) throws IOException {
		
		columnDetails = columnDetails.substring(0, columnDetails.length()-1);
		String columns[] = columnDetails.split(",");
		for (int i = 0; i < columns.length; i++) {
			if(calcFreeSpace(recordLength, rfColPos, numberOfRecordsPage, countPages)){
				//colname, data_type, pri, nullable
				seek(pageSize*(countPages-1)+1);
				//System.out.println(countPages);
				//nPage = readByte();//number of records on current page
				//System.out.println("num of records : "+numberOfRecordsPage);

				++this.numberOfRecords;
				++this.numberOfRecordsPage;
				seek(pageSize*(countPages-1)+1);
				writeByte(numberOfRecordsPage);
				
				
				
				rfColPos = rfColPos - recordLength;
				//System.out.println(recordLength);
				//System.out.println("position of latest data: "+rfColPos);
				//Set Address for each record
				seek(pageSize*(countPages-1)+recordStartPostion);
				//System.out.println("enter location here: "+(pageSize*(countPages-1)+recordStartPostion));
				writeShort(rfColPos);
				//System.out.println("if writing "+rfColPos+" to "+(pageSize*(countPages-1)+recordStartPostion));
				recordStartPostion += 2;
				
				seek(rfColPos);
				writeShort(75);
				seek(rfColPos+2);
				writeInt(++rowId);

				numberOfColumns = 6;

				seek(rfColPos+6);
				writeByte(numberOfColumns);
				seek(rfColPos+7);
				writeByte(6);
				seek(rfColPos+8);
				writeByte(20);
				seek(rfColPos+9);
				writeByte(20);
				seek(rfColPos+10);
				writeByte(20);
				seek(rfColPos+11);
				writeByte(4);
				seek(rfColPos+12);
				writeByte(4);

				seek(rfColPos+13);
				writeBytes(tableName);
				
				
				String temp[] = new String[3];
				
				String temp1[] = columns[i].split(" ");
				
				for (int j = 0; j < temp1.length; j++) {
					temp[j] = temp1[j];
				}
				seek(rfColPos+33);
				writeBytes(temp[0]); //ColumnName
				
				seek(rfColPos+53);
				writeBytes(temp[1].toUpperCase()); //DataType


				seek(rfColPos+73);
				writeByte(i+1);						  //OrdinalPosition

				/*
				if(temp[2] != null && temp[2].equalsIgnoreCase("PRI"))
					writeBytes(temp[2]); //ColumnKey
				else {
					writeBytes("");
					temp[3] = temp[2];
				}
				seek(rfColPos+63);
				*/
				

				if(temp[2].equalsIgnoreCase("not_null")){
					seek(rfColPos+77);
					writeBytes("NO");
				}else{
					seek(rfColPos+77);
					writeBytes("YES");
				}

				seek(pageSize*(countPages-1)+2);
				writeShort(rfColPos);

				seek(pageSize*(countPages-1)+4);
				writeInt(-1);
			}else{
				//create a new page and add data to the root page

				rootPos = rootPos - 8;
				seek(rootPos);
				writeInt(countPages);
				seek(rootPos+4);
				writeInt(rowId);
				rootNumberOfRecords++;
				seek(1025);
				writeByte(rootNumberOfRecords);
				seek(1024+rootRecordStartPostion);
				writeShort(rootPos);
				rootRecordStartPostion +=2;

				if(countPages==2){
					//2nd page is filled up, go directly to 4th page
					//System.out.println("adding data to root page");
					seek(pageSize*(countPages-1)+4);
					writeInt(4);	
					countPages++;
				}else{
					seek(pageSize*(countPages-1)+4);
					writeInt(countPages+1);	
				}

				

				countPages++;
				numberOfRecordsPage = 0;
				setLength(pageSize*countPages);
				rfColPos = pageSize*countPages;
				//System.out.println(rfColPos);
				recordStartPostion = 8;

				seek(pageSize*(countPages-1));
				writeByte(13);

				seek(pageSize*(countPages-1)+1);
				//System.out.println(numberOfRecords);
				numberOfRecords += 1;
				numberOfRecordsPage +=1;
				writeByte(numberOfRecordsPage);

				rfColPos = rfColPos - recordLength;
				//System.out.println(rfColPos);
				//System.out.println(rfColPos);
				
				//Set Address of last record inserted
				seek(pageSize*(countPages-1)+2);
				writeShort(rfColPos);
				
				seek(pageSize*(countPages-1) + recordStartPostion);
				writeShort(rfColPos);
				//System.out.println("else writing "+rfColPos+" to "+recordStartPostion);
				recordStartPostion += 2;
				
				seek(rfColPos);
				writeShort(75);
				seek(rfColPos+2);
				writeInt(++rowId);

				numberOfColumns = 6;

				seek(rfColPos+6);
				writeByte(numberOfColumns);
				seek(rfColPos+7);
				writeByte(6);
				seek(rfColPos+8);
				writeByte(20);
				seek(rfColPos+9);
				writeByte(20);
				seek(rfColPos+10);
				writeByte(20);
				seek(rfColPos+11);
				writeByte(4);
				seek(rfColPos+12);
				writeByte(20);

				seek(rfColPos+13);
				writeBytes(tableName);

				String temp[] = new String[3];
				
				String temp1[] = columns[i].split(" ");
				
				for (int j = 0; j < temp1.length; j++) {
					temp[j] = temp1[j];
				}
				seek(rfColPos+33);
				writeBytes(temp[0]); //ColumnName
				
				seek(rfColPos+53);
				writeBytes(temp[1].toUpperCase()); //DataType


				seek(rfColPos+73);
				writeByte(i+1);						  //OrdinalPosition

				/*
				if(temp[2] != null && temp[2].equalsIgnoreCase("PRI"))
					writeBytes(temp[2]); //ColumnKey
				else {
					writeBytes("");
					temp[3] = temp[2];
				}
				seek(rfColPos+63);
				*/
				
				if(temp[2].equalsIgnoreCase("not_null")){
					seek(rfColPos+77);
					writeBytes("NO");
				}else{
					seek(rfColPos+77);
					writeBytes("YES");
				}
				

				seek(pageSize*(countPages-1)+2);
				writeShort(rfColPos);
			}	
		}		
	}


	public void processDropString(String tableName, CreateFile rfTbl, CreateFile rfCol) throws IOException {
		

		//deleting the entry from tables TABLE
		int remove = 0;
		rfTbl.seek(1);
		int n = rfTbl.readByte();
		//System.out.println(n);
		int start = 8;
		int[] addresses = new int[n];
		
		for(int i=0;i<n;i++){
			rfTbl.seek(start);
			addresses[i] = rfTbl.readShort();
			rfTbl.seek(start);
			rfTbl.writeShort(0);
			start += 2;
		}
		for(int i=0;i<n;i++){
			rfTbl.seek(addresses[i]+9);
			String rfTblReferenceTableLine = rfTbl.readLine().substring(0, 20).trim();
			if(rfTblReferenceTableLine.equalsIgnoreCase(tableName)){
				//System.out.println("Table name found at: "+addresses[i]);
				remove = i;
				break;
			}
		}
		
		for(int i=remove;i<n-1;i++){
			addresses[i] = addresses[i+1];
		}

		start = 8;
		n--;
		for(int i=0;i<n;i++){
			rfTbl.seek(start);
			rfTbl.writeShort(addresses[i]);
			start+=2;
		}

		//change count for number of Records
		rfTbl.seek(1);
		rfTbl.writeByte(n);

		//change address of latest record
		rfTbl.seek(2);
		rfTbl.writeShort(addresses[n-1]);		

		//deleting the entries from the columns TABLE
		int[] rem;
		int ct = 0;
		for(int i=1; i<=rfCol.countPages;i++){
			rfCol.seek(rfCol.pageSize*(i-1)+1);
			n = rfCol.readByte();
			rem = new int[n];
			start = rfCol.pageSize*(i-1)+8;
			addresses = new int[n];
			//get addresses of all records of current page into array
			for(int j=0;j<n;j++){
				rfCol.seek(start);
				addresses[j] = rfCol.readShort();
				//rfCol.seek(start);
				//rfCol.writeShort(0);
				start += 2;
			}
			ct = 0;
			//check if table to be dropped has any record in current page
			for(int m=0;m<n;m++){
				rfCol.seek(addresses[m]+13);
				String rfColReferenceTableLine = rfCol.readLine().substring(0, 20).trim();
				if(rfColReferenceTableLine.equalsIgnoreCase(tableName)){
					//System.out.println("Table name found at: "+addresses[m]);
					//remove = i;
					//break;
					//System.out.println(ct);
					rem[ct] = m;
					ct++;
				}
			}
			//System.out.println(ct);
			if(ct!=0){
				//there are records to be removed from current page
				//make the addresses all zero on the page
				start = rfCol.pageSize*(i-1)+8;
				for(int k=0;k<n;k++){
					rfCol.seek(start);
					//rfCol.seek(start);
					rfCol.writeShort(0);
					start += 2;
				}
				int diff = rem[ct-1]-rem[0];
				int l = n-diff-1;
				for(int q=rem[0]; q<l;q++){
					addresses[q] = addresses[q+diff+1];
					n--; 
				}
				for(int u=0;u<l;u++){
					//System.out.print(addresses[u]+" ");
				}
				//writing the shortened addresses
				start = rfCol.pageSize*(i-1)+8;
				for(int y=0;y<l;y++){
					rfCol.seek(start);
					rfCol.writeShort(addresses[y]);
					start+=2;
				}
				//change count for number of Records
				rfCol.seek(rfCol.pageSize*(i-1)+1);
				rfCol.writeByte(l);

				//change address of latest record
				rfCol.seek(rfCol.pageSize*(i-1)+2);
				rfCol.writeShort(addresses[l-1]);
			}
		}

			
		System.out.println("Query OK, 0 rows affected\n");
	}

	public void processInsertQuery(String tableName, String[] columnList, String[] columnValues, CreateFile rfTbl, CreateFile rfCol, BPlusTree tree) throws IOException {
		/*add checks for row_id part of columnList and that not null values have been 
		given in the columnValues*/
		if(countPages == 1) {
			seek(1);
			numberOfRecordsPage = readByte();
		} else {
			seek(pageSize + (countPages-2)*512 + 1);
			numberOfRecordsPage = readByte();
		}

		int add_record = 0, rSP = 0;
		int n;
		String referenceTableLine, dataType;
		
		/*for(int j=0;j<cols_dt.size();j++){
			System.out.println(cols_dt.get(j));	
		}*/
		/*for(int i=0;i<columnList.length();i++){
			recordLength += getRecordLengthForDataType(columnList[i]);
		}*/
		
		//System.out.println("record length of user table : "+recordLength);

		if(Integer.parseInt(columnValues[0])<=numberOfRecords){
			//duplicate entry of row_id is being entered,
			//throw an error
			System.out.println("Duplicate entry for key 'PRIMARY'\n");
			return;
		}
		else{
			if(numberOfRecordsPage==0){
				//no record has been entered into the table yet
				recordLength = recordLength + 7 + numberOfColumns;
				numberOfRecordsPage++;
				seek(1);
				writeByte(numberOfRecordsPage);

				//no need to check if the page has free space,
				//enter the page header values and the data directly

				pos = pos - recordLength;
				seek(2);
				writeShort(pos);

				seek(recordStartPostion);
				writeShort(pos);

				seek(4);
				writeInt(-1);
				seek(pos);
				writeShort(recordLength-6);
				seek(pos+2);
				writeInt(++rowId);
				seek(pos+6);
				writeByte(numberOfColumns);
				
				for(int m=0;m<cols_dt.size();m++){
					seek(pos+m+7);
					writeByte(getSerialCode(cols_dt.get(m).toString()));
				}

				pos = pos + numberOfColumns + 7;

				for(int k=0;k<columnValues.length;k++){
					//write the actual data entered by the user into the file
					String dt_string = cols_dt.get(k).toString();

					if(dt_string.equalsIgnoreCase("int")){
							//pos = pos - 4;
							seek(pos);
							if("null".equalsIgnoreCase(columnValues[k]))
								writeInt(0);
							else
								writeInt((Integer.parseInt(columnValues[k])));
							pos +=4;
							//seek(pos+4);
						} 
						else if(dt_string.equalsIgnoreCase("tinyint"))
				        {
							//pos = pos - 1;
							seek(pos);
							if("null".equalsIgnoreCase(columnValues[k]))
								writeByte(0);
							else
								writeByte((Integer.parseInt(columnValues[k])));
							pos +=1;
							
				        }
				        else if(dt_string.equalsIgnoreCase("smallint"))
				        {
				        	//pos = pos - 2;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
								writeShort(0);
							else
								writeShort((Short.parseShort(columnValues[k])));
							pos +=2;
				        }
				        else if(dt_string.equalsIgnoreCase("bigint"))
				        {
				        	//pos = pos - 8;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
								writeLong(0);
							else
								writeLong((Long.parseLong(columnValues[k])));
							pos +=8;
				        }
				        else if(dt_string.equalsIgnoreCase("real"))
				        {
				        	//pos = pos - 4;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
								writeFloat(0);
							else
								writeFloat((Float.parseFloat(columnValues[k])));
							pos +=4;
				        }
				        else if(dt_string.equalsIgnoreCase("double"))
				        {
				        	//pos = pos - 8;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
				        		writeDouble(0);
							else
								writeDouble((Double.parseDouble(columnValues[k])));
							pos +=8;
				        }
				        else if(dt_string.equalsIgnoreCase("datetime"))
				        {
				        	//pos = pos - 8;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
				        		writeLong(0);
				        	else {
				        		String dateParams[] = columnValues[k].split("-");
				        		ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
				        		writeLong ( epochSeconds );
				        		
				        	}
							pos +=8;
				        }
				        else if(dt_string.equalsIgnoreCase("date"))
				        {
				        	//pos = pos - 8;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
				        		writeLong(0);
				        	else {
				        		String dateParams[] = columnValues[k].split("-");
				        		ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
				        		writeLong ( epochSeconds );
				        		
				        	}
							pos +=8;
				        }
				        else if(dt_string.equalsIgnoreCase("text"))
				        {
				        	//pos = pos - 20;
				        	seek(pos);
				        	if("null".equalsIgnoreCase(columnValues[k]))
				        		writeBytes("");
				        	else
				        		writeBytes(columnValues[k]);
							pos +=20;
				        }
				        
				}
				seek(2);
				pos = readShort();
				recordStartPostion+=2;
				numberOfRecords++;
			}else{
				//check if current page has free space available
				if(calcFreeSpace(recordLength, pos, numberOfRecordsPage, countPages)){

					//get number of records on current page, increment it and write back
					seek(pageSize*(countPages-1)+1);
					numberOfRecordsPage = readByte();
					numberOfRecordsPage++;
					seek(1);
					writeByte(numberOfRecordsPage);


					pos = pos - recordLength;
					//System.out.println("Current record being written at: "+pos + " rl "+recordLength);
					seek(pageSize*(countPages-1)+2);
					writeShort(pos);

					seek(pageSize*(countPages-1)+recordStartPostion);
					writeShort(pos);

					seek(pos);
					writeShort(recordLength-6);
					seek(pos+2);
					writeInt(++rowId);
					seek(pos+6);
					writeByte(numberOfColumns);
					
					for(int m=0;m<cols_dt.size();m++){
						seek(pos+m+7);
						writeByte(getSerialCode(cols_dt.get(m).toString()));
						//System.out.println("writing "+cols_dt.get(m).toString()+" as serial code");
					}

					pos = pos + numberOfColumns + 7;

					for(int k=0;k<columnValues.length;k++){
						//write the actual data entered by the user into the file
						String dt_string = cols_dt.get(k).toString();

							if(dt_string.equalsIgnoreCase("int")){
								//pos = pos - 4;
								seek(pos);
								if("null".equalsIgnoreCase(columnValues[k]))
									writeInt(0);
								else
									writeInt((Integer.parseInt(columnValues[k])));
								pos +=4;
								//seek(pos+4);
							} 
							else if(dt_string.equalsIgnoreCase("tinyint"))
					        {
								//pos = pos - 1;
								seek(pos);
								if("null".equalsIgnoreCase(columnValues[k]))
									writeByte(0);
								else
									writeByte((Integer.parseInt(columnValues[k])));
								pos +=1;
								
					        }
					        else if(dt_string.equalsIgnoreCase("smallint"))
					        {
					        	//pos = pos - 2;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
									writeShort(0);
								else
									writeShort((Short.parseShort(columnValues[k])));
								pos +=2;
					        }
					        else if(dt_string.equalsIgnoreCase("bigint"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
									writeLong(0);
								else
									writeLong((Long.parseLong(columnValues[k])));
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("real"))
					        {
					        	//pos = pos - 4;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
									writeFloat(0);
								else
									writeFloat((Float.parseFloat(columnValues[k])));
								pos +=4;
					        }
					        else if(dt_string.equalsIgnoreCase("double"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeDouble(0);
								else
									writeDouble((Double.parseDouble(columnValues[k])));
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("datetime"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeLong(0);
					        	else {
					        		String dateParams[] = columnValues[k].split("-");
					        		ZoneId zoneId = ZoneId.of( "America/Chicago");
					        		
					        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
					        		
					        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
					        		/* ZonedDateTime toLocalDate() method will display in a simple format */
					        		//System.out.println(zdt.toLocalDate()); 
					        		
					        		/* Convert a ZonedDateTime object to epochSeconds
					        		 * This value can be store 8-byte integer to a binary
					        		 * file using RandomAccessFile writeLong()
					        		 */
					        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        		writeLong ( epochSeconds );
					        		
					        	}
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("date"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeLong(0);
					        	else {
					        		String dateParams[] = columnValues[k].split("-");
					        		ZoneId zoneId = ZoneId.of( "America/Chicago");
					        		
					        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
					        		
					        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
					        		/* ZonedDateTime toLocalDate() method will display in a simple format */
					        		//System.out.println(zdt.toLocalDate()); 
					        		
					        		/* Convert a ZonedDateTime object to epochSeconds
					        		 * This value can be store 8-byte integer to a binary
					        		 * file using RandomAccessFile writeLong()
					        		 */
					        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        		writeLong ( epochSeconds );
					        		
					        	}
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("text"))
					        {
					        	//pos = pos - 20;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeBytes("");
					        	else
					        		writeBytes(columnValues[k]);
								pos +=20;
					        }
					        
					}
					seek(pageSize*(countPages-1)+2);
					pos = readShort();
					//System.out.println("Current record written at: "+pos);
					recordStartPostion+=2;
					numberOfRecords++;
				}
				else{
					//no free space available
					if(countPages==1){
						//first page has overflowed, create the rootpage

						setLength(pageSize*3);
						seek(1024);
						writeByte(5);

						rootNumberOfRecords++;
						seek(1025);
						writeByte(rootNumberOfRecords);

						rootPos = 1536;
						rootPos -=8;
						seek(rootPos);
						writeInt(countPages);
						seek(rootPos+4);
						writeInt(numberOfRecordsPage);
						seek(1024 + rootRecordStartPostion);
						writeShort(rootPos);
						rootRecordStartPostion +=2;

						seek(4);
						writeInt(countPages+1);

						countPages++;
					}else{
						//page number other than 1 is filled

						rootPos = rootPos - 8;
						seek(rootPos);
						writeInt(countPages);
						seek(rootPos+4);
						writeInt(rowId);
						rootNumberOfRecords++;
						seek(1025);
						writeByte(rootNumberOfRecords);
						seek(1024 + rootRecordStartPostion);
						writeShort(rootPos);
						rootRecordStartPostion +=2;

						if(countPages==2){
							seek(pageSize*(countPages-1)+4);
							writeInt(4);	
							countPages++;
						}else{
							seek(pageSize*(countPages-1)+4);
							writeInt(countPages+1);
						}
						countPages++;
					}
					//adding the actual data to the new leaf page

					seek(pageSize*(countPages-1));
					writeByte(13);
					numberOfRecordsPage = 0;
					numberOfRecordsPage++;
					seek(pageSize*(countPages-1)+1);
					writeByte(numberOfRecordsPage);

					pos = pageSize*countPages;
					pos = pos - recordLength;
					seek(pageSize*(countPages-1)+2);
					writeShort(pos);

					recordStartPostion = 8;
					seek(pageSize*(countPages-1)+recordStartPostion);
					writeShort(pos);
					
					seek(pos);
					writeShort(recordLength-6);
					seek(pos+2);
					writeInt(++rowId);
					seek(pos+6);
					writeByte(numberOfColumns);

					for(int m=0;m<cols_dt.size();m++){
						seek(pos+m+7);
						writeByte(getSerialCode(cols_dt.get(m).toString()));
						//System.out.println("writing "+cols_dt.get(m).toString()+" as serial code");
					}

					pos = pos + numberOfColumns + 7;

					for(int k=0;k<columnValues.length;k++){
						//write the actual data entered by the user into the file
						String dt_string = cols_dt.get(k).toString();

							if(dt_string.equalsIgnoreCase("int")){
								//pos = pos - 4;
								seek(pos);
								if("null".equalsIgnoreCase(columnValues[k]))
									writeInt(0);
								else
									writeInt((Integer.parseInt(columnValues[k])));
								pos +=4;
								//seek(pos+4);
							} 
							else if(dt_string.equalsIgnoreCase("tinyint"))
					        {
								//pos = pos - 1;
								seek(pos);
								if("null".equalsIgnoreCase(columnValues[k]))
									writeByte(0);
								else
									writeByte((Integer.parseInt(columnValues[k])));
								pos +=1;
								
					        }
					        else if(dt_string.equalsIgnoreCase("smallint"))
					        {
					        	//pos = pos - 2;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
									writeShort(0);
								else
									writeShort((Short.parseShort(columnValues[k])));
								pos +=2;
					        }
					        else if(dt_string.equalsIgnoreCase("bigint"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
									writeLong(0);
								else
									writeLong((Long.parseLong(columnValues[k])));
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("real"))
					        {
					        	//pos = pos - 4;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
									writeFloat(0);
								else
									writeFloat((Float.parseFloat(columnValues[k])));
								pos +=4;
					        }
					        else if(dt_string.equalsIgnoreCase("double"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeDouble(0);
								else
									writeDouble((Double.parseDouble(columnValues[k])));
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("datetime"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeLong(0);
					        	else {
					        		String dateParams[] = columnValues[k].split("-");
					        		ZoneId zoneId = ZoneId.of( "America/Chicago");
					        		
					        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
					        		
					        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
					        		/* ZonedDateTime toLocalDate() method will display in a simple format */
					        		//System.out.println(zdt.toLocalDate()); 
					        		
					        		/* Convert a ZonedDateTime object to epochSeconds
					        		 * This value can be store 8-byte integer to a binary
					        		 * file using RandomAccessFile writeLong()
					        		 */
					        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        		writeLong ( epochSeconds );
					        		
					        	}
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("date"))
					        {
					        	//pos = pos - 8;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeLong(0);
					        	else {
					        		String dateParams[] = columnValues[k].split("-");
					        		ZoneId zoneId = ZoneId.of( "America/Chicago");
					        		
					        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
					        		
					        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
					        		/* ZonedDateTime toLocalDate() method will display in a simple format */
					        		//System.out.println(zdt.toLocalDate()); 
					        		
					        		/* Convert a ZonedDateTime object to epochSeconds
					        		 * This value can be store 8-byte integer to a binary
					        		 * file using RandomAccessFile writeLong()
					        		 */
					        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        		writeLong ( epochSeconds );
					        		
					        	}
								pos +=8;
					        }
					        else if(dt_string.equalsIgnoreCase("text"))
					        {
					        	//pos = pos - 20;
					        	seek(pos);
					        	if("null".equalsIgnoreCase(columnValues[k]))
					        		writeBytes("");
					        	else
					        		writeBytes(columnValues[k]);
								pos +=20;
					        }
					        
					}
					seek(pageSize*(countPages-1)+2);
					pos = readShort();
					recordStartPostion+=2;
					numberOfRecords++;
				}
			}
			System.out.println("Query OK, 1 row affected\n");
		}
		
	}

	public void processSelectQuery(String tableName, CreateFile rfCol) throws IOException{

		int n,r_start,r_pos;
		String data_type;
		ArrayList<String> cols = new ArrayList<>();

		for(int i=1;i<=rfCol.countPages;i++){
			if(i!=3){
				rfCol.seek(rfCol.pageSize*(i-1)+1);
				n = rfCol.readByte();
				r_start = rfCol.pageSize*(i-1)+8;

				for(int k=0;k<n;k++){
					rfCol.seek(r_start);
					rfColPos = rfCol.readShort();
					rfColPos +=13;

					rfCol.seek(rfColPos);
					String referenceTableLine = rfCol.readLine().substring(0,20).trim();
					if(referenceTableLine.equalsIgnoreCase(tableName)){
						rfColPos +=20;
						rfCol.seek(rfColPos);
						cols.add(rfCol.readLine().substring(0,20).trim());
					}
					r_start +=2;
				}

			}
		}

		for(int k=0;k<cols.size();k++){
			System.out.print("+---------------");
		}
		System.out.println("+");
		for(int k=0;k<cols.size();k++){
			System.out.print("|\t"+cols.get(k)+"\t");
		}
		System.out.print("|");
		System.out.println();
		for(int k=0;k<cols.size();k++){
			System.out.print("+---------------");
		}
		System.out.println("+");
		for(int i=1;i<=countPages;i++){
			if(i!=3){
				seek(pageSize*(i-1)+1);
				n = readByte();
				r_start = pageSize*(i-1)+8;
				
				for(int k=0;k<n;k++){
					seek(r_start);
					r_pos = readShort();
					r_pos += 7+numberOfColumns;

					for(int m=0;m<cols_dt.size();m++){
						data_type = cols_dt.get(m);
						System.out.print("|");
						if(data_type.equalsIgnoreCase("int")){
							seek(r_pos);
							System.out.print("\t"+readInt()+"\t");
							r_pos +=4;
						}else if(data_type.equalsIgnoreCase("text")){
							seek(r_pos);
							System.out.print("\t"+readLine().substring(0,20).trim()+"\t");
							r_pos +=20;
						}	
					}
					System.out.print("|");
					System.out.println();
					r_start +=2;
				}
			}
		}
		for(int k=0;k<cols.size();k++){
			System.out.print("+---------------");
		}
		System.out.println("+");	
		System.out.println();

	}
	public void processSelectQuery(String tableName, String[] columnList, CreateFile rfCol) throws IOException{

		int n,r_start;
		String data_type;

		if(columnList.length == 1){
			//select only one column
		}else{

		}
	}
}
