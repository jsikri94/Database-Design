		import java.io.RandomAccessFile;
		import java.io.File;
		import java.io.FileReader;
		import java.util.Scanner;
		import java.util.SortedMap;
		import java.util.ArrayList;
		import java.util.Arrays;
		import java.io.IOException;
		import java.util.HashMap;
		import java.util.Map;
		import static java.lang.System.out;

		/**
		 *  @author Chris Irwin Davis
		 *  @version 1.0
		 *  <b>
		 *  <p>This is an example of how to create an interactive prompt</p>
		 *  <p>There is also some guidance to get started wiht read/write of
		 *     binary data files using RandomAccessFile class</p>
		 *  </b>
		 *
		 */
		public class DavisBaseProgram {

			/* This can be changed to whatever you like */
			static String prompt = "davisql> ";
			static String version = "v1.0b(example)";
			static String copyright = "©2016 Jaahanavee Sikri";
			static boolean isExit = false;
			/*
			 * Page size for alll files is 512 bytes by default.
			 * You may choose to make it user modifiable
			 */
			static int pageSize = 512; 
			static CreateFile rfCol,rfTbl;
			static Map<String, BPlusTree> bPlusTreeMap = new HashMap<String, BPlusTree>();

			/* 
			 *  The Scanner class is used to collect user commands from the prompt
			 *  There are many ways to do this. This is just one.
			 *
			 *  Each time the semicolon (;) delimiter is entered, the queryString 
			 *  String is re-populated.
			 */
			static Scanner scanner = new Scanner(System.in).useDelimiter(";");
			
			/** ***********************************************************************
			 *  Main method
			 */
		    public static void main(String[] args) throws IOException{

				/* Display the welcome screen */
				splashScreen();

				/*Initialize the meta data, davisbase_tables and davisbase_columns*/
				initializeMetaData();

				/* Variable to collect user input from the prompt */
				String queryString = ""; 

				while(!isExit) {
					System.out.print(prompt);
					/* toLowerCase() renders command case insensitive */
					queryString = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
					// queryString = queryString.replace("\n", "").replace("\r", "");
					parseUserCommand(queryString);
				}
				System.out.println("Exiting...");


			}

			/** ***********************************************************************
			 *  Static method definitions
			 */

			/**
			 *  Display the splash screen
			 */
			public static void splashScreen() {
				System.out.println(line("-",80));
		        System.out.println("Welcome to DavisBaseLite"); // Display the string.
				System.out.println("DavisBaseLite Version " + getVersion());
				System.out.println(getCopyright());
				System.out.println("\nType \"help;\" to display supported commands.");
				System.out.println(line("-",80));
			}
			
			/**
			 * @param s The String to be repeated
			 * @param num The number of time to repeat String s.
			 * @return String A String object, which is the String s appended to itself num times.
			 */
			public static String line(String s,int num) {
				String a = "";
				for(int i=0;i<num;i++) {
					a += s;
				}
				return a;
			}
			
			public static void printCmd(String s) {
				System.out.println("\n\t" + s + "\n");
			}
			public static void printDef(String s) {
				System.out.println("\t\t" + s);
			}
			
				/**
				 *  Help: Display supported commands
				 */
				public static void help() {
					out.println(line("*",80));
					out.println("SUPPORTED COMMANDS\n");
					out.println("All commands below are case insensitive\n");
					out.println("SHOW TABLES;");
					out.println("\tDisplay the names of all tables.\n");
					//printCmd("SELECT * FROM <table_name>;");
					//printDef("Display all records in the table <table_name>.");
					out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
					out.println("\tDisplay table records whose optional <condition>");
					out.println("\tis <column_name> = <value>.\n");
					out.println("DROP TABLE <table_name>;");
					out.println("\tRemove table data (i.e. all records) and its schema.\n");
					out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
					out.println("\tModify records data whose optional <condition> is\n");
					out.println("VERSION;");
					out.println("\tDisplay the program version.\n");
					out.println("HELP;");
					out.println("\tDisplay this help information.\n");
					out.println("EXIT;");
					out.println("\tExit the program.\n");
					out.println(line("*",80));
				}

			/** return the DavisBase version */
			public static String getVersion() {
				return version;
			}
			
			public static String getCopyright() {
				return copyright;
			}
			
			public static void displayVersion() {
				System.out.println("DavisBaseLite Version " + getVersion());
				System.out.println(getCopyright());
			}
				
			public static void initializeMetaData() throws IOException{
				File mainDir = new File("Data");

				 if(!mainDir.exists())
				 {	/*Data folder does not exist, create one and create the
					davisbase_tables.tbl and davisbase_columns.tbl and fill it with
					initial data*/
					mainDir.mkdir();
					 
					File subDirectoryOne = new File("Data\\catalog");
					subDirectoryOne.mkdir();
					
					File subDirectoryTwo = new File("Data\\user_data");
					subDirectoryTwo.mkdir();
					
					//Create 
					File fileTbl = new File("Data\\catalog\\davisbase_tables.tbl");			
					rfTbl = new CreateFile(fileTbl, "rw", 512);
					rfTbl.setLength(512);
					rfTbl.insertIntoDavisBaseTable("davisbase_tables");
					rfTbl.insertIntoDavisBaseTable("davisbase_columns");
					
					
					File fileCol = new File("Data\\catalog\\davisbase_columns.tbl");
					rfCol = new CreateFile(fileCol, "rw", 512);	
					
					rfCol.insertIntoDavisBaseColumn("davisbase_tables", "rowid", "INT", 1, "NOT_NULL");
					rfCol.insertIntoDavisBaseColumn("davisbase_tables", "table_name", "TEXT", 2, "NOT_NULL");
					
					rfCol.insertIntoDavisBaseColumn("davisbase_columns", "rowid", "INT",  1, "NOT_NULL");
					rfCol.insertIntoDavisBaseColumn("davisbase_columns", "table_name", "TEXT",  2, "NOT_NULL");
					rfCol.insertIntoDavisBaseColumn("davisbase_columns", "column_name", "TEXT", 3, "NOT_NULL");
					rfCol.insertIntoDavisBaseColumn("davisbase_columns", "data_type", "TEXT", 4, "NOT_NULL");
					rfCol.insertIntoDavisBaseColumn("davisbase_columns", "ordinal_position", "TINYINT",  5, "NOT_NULL");
					rfCol.insertIntoDavisBaseColumn("davisbase_columns", "is_nullable", "TEXT",  6, "NOT_NULL");
					
							
				 }
				  else  //If files already exists
				 {	
				 	/*Data folder already exists, create files to refer to them*/
					File fileTbl = new File("Data\\catalog\\davisbase_tables.tbl");			
					rfTbl = new CreateFile(fileTbl, "rw", 512);
					rfTbl.recordLength = 29;
					rfTbl.numberOfColumns = 2;
					rfTbl.seek(2);
					rfTbl.rfTblPos = rfTbl.readShort();
					rfTbl.numberOfRecords = rfTbl.calculateNumberOfRecords(1);
					rfTbl.rowId = rfTbl.numberOfRecords;
					rfTbl.recordStartPostion = 8 + rfTbl.numberOfRecords*2;
							
					File fileCol = new File("Data\\catalog\\davisbase_columns.tbl");
					rfCol = new CreateFile(fileCol, "rw",512);

					rfCol.recordLength = 81;
					rfCol.numberOfColumns = 6;
					rfCol.countPages = (int) (rfCol.length()/512);
		
					rfCol.numberOfRecords = rfCol.calculateNumberOfRecords(rfCol.countPages);
					rfCol.seek(pageSize*(rfCol.countPages-1)+1);
					rfCol.numberOfRecordsPage = rfCol.readByte();
					rfCol.rowId = rfCol.numberOfRecords;
					rfCol.recordStartPostion = pageSize*(rfCol.countPages-1) + 8 + rfCol.numberOfRecordsPage*2;
					rfCol.rootNumberOfRecords = rfCol.getRootRecords();
					rfCol.rootPos = 1536 - rfCol.rootNumberOfRecords*8;
					
					rfTbl.seek(8);
					int start = rfTbl.readShort() + 9;
					rfTbl.seek(1);
					int count = rfTbl.readByte();
					
					while(count != 0) {
						rfTbl.seek(start);
						String tableName = rfTbl.readLine().substring(0, 20).trim();
						
						//creating files for tables other than davisbase_tables and davisbase_columns 
						if(!(tableName.equalsIgnoreCase("davisbase_tables")) && !(tableName.equalsIgnoreCase("davisbase_columns"))) {
							File file = new File("Data\\user_data\\" + tableName + ".tbl");
							CreateFile newTable = new CreateFile(file, "rw", pageSize);
							
							newTable.countPages = (int) (newTable.length()/512); //countPages
							newTable.numberOfRecords = newTable.calculateNumberOfRecords(newTable.countPages); //NumberOfRecords
							newTable.rowId = newTable.numberOfRecords;
							newTable.numberOfRecordsPage = newTable.calculateNumberOfRecordsPage(newTable.countPages);
							newTable.recordStartPostion = 8+newTable.numberOfRecordsPage*2;

							int values[] = rfCol.calculateNumberOfColumnsAndRecordLength(tableName); //recordLength, numberOfColumns
							newTable.numberOfColumns = values[0];
							newTable.recordLength = values[1]+7+newTable.numberOfColumns;
							newTable.cols_dt = rfCol.getColsDt(tableName);

							newTable.pos = (int)newTable.length() - newTable.numberOfRecordsPage*newTable.recordLength;

							if(newTable.countPages>1){
								newTable.rootNumberOfRecords = newTable.getRootRecords();
								newTable.rootPos = 1536 - newTable.rootNumberOfRecords*8;
							}
							//System.out.println(tableName+" "+newTable.length()+" "+newTable.numberOfColumns+" "+newTable.recordLength);
							BPlusTree bPlusTree = new BPlusTree(newTable);
							bPlusTreeMap.put(tableName, bPlusTree);
						}
						start = start - rfTbl.recordLength;
						count--;
					}//while loop
				}//else
			}//	
			public static void parseUserCommand (String queryString) throws IOException{
				
				/* commandTokens is an array of Strings that contains one token per array element 
				 * The first token can be used to determine the type of command 
				 * The other tokens can be used to pass relevant parameters to each command-specific
				 * method inside each case statement */
				// String[] commandTokens = queryString.split(" ");
				ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
				

				/*
				*  This switch handles a very small list of hardcoded commands of known syntax.
				*  You will want to rewrite this method to interpret more complex commands. 
				*/
				switch (commandTokens.get(0)) {
					//DDL Commands 
					case "show":
						//System.out.println("CASE: SHOW");
						displayTables();
						break;
					case "create":
						//System.out.println("CASE: CREATE");
						parseCreateTable(queryString);
						break;	
					case "drop":
						//System.out.println("CASE: DROP");
						dropTable(queryString);
						break;
						
					//DML Commands
					case "insert":
						//System.out.println("CASE: INSERT");
						parseInsert(queryString);
						break;


					//VDL Commands			
					case "select":
						//System.out.println("CASE: SELECT");
						parseSelectQuery(queryString);
						break;
					case "exit":
						isExit = true;
						break;


					case "help":
						help();
						break;
					case "version":
						displayVersion();
						break;
					case "quit":
						isExit = true;
					default:
						System.out.println("I didn't understand the command: \"" + queryString + "\"");
						break;
				}
			}
			


			/*
			Method to display the tables
			show tables;
			*/
			private static void displayTables() throws IOException {
				rfTbl.processShowTableQuery();
			}

			/*
			Method for creating a table
			create table tableName (row_id int not_null,columnName dataTyp null/not_null,....)*/
			private static void parseCreateTable(String queryString) throws IOException {
				String commandElements[] = queryString.split("\\(");
				String fetchTableName[] = commandElements[0].trim().split(" ");
				String tableName = fetchTableName[2];
				//System.out.println("Table Name: " + tableName);
				if(rfTbl.checkIfTableAlreadyExists(tableName)) {
					System.out.println("Table '"+tableName+"' already exists\n");
					return;
				}

				String columnDetails = commandElements[1].substring(0, commandElements[1].length()-1);
				String columns[] = columnDetails.split(",");
				
				String t[] = columns[0].split(" ");
				if(!t[0].equalsIgnoreCase("row_id")){
					System.out.println("'row_id' column not provided\n");
					return;
				}
				
				rfTbl.insertIntoDavisBaseTable(tableName);
				rfCol.insertUserColumsEntry(tableName, commandElements[1]);
				
				File f = new File("Data\\user_data\\" + tableName + ".tbl");
				CreateFile newTable = new CreateFile(f, "rw", pageSize);
				newTable.setLength(pageSize);
				
				newTable.seek(4);
				newTable.writeInt(-1);
				
				//Set root pageType
				int rootStart = pageSize - 512;
				newTable.seek(rootStart);
				newTable.writeByte(5);
				
				//Pointing root to first left leaf node
				newTable.seek(rootStart + 4);
				newTable.writeInt(0);
				
				//Set page type
				newTable.setPageType(13);
				
				//Initialize the record length for each new table
				newTable.calculateRecordLength(commandElements[1]);
				//System.out.println(newTable.recordLength);
				
				newTable.cols_dt = rfCol.getColsDt(tableName);
				//Create a BPlusTree
				BPlusTree bPlusTree = new BPlusTree(newTable);

				bPlusTreeMap.put(tableName, bPlusTree);
				
				System.out.println("Query OK, 0 rows affected\n");
			}
			/**
			 *  Stub method for dropping tables
			 *  @param dropTableString is a String of the user input
			 */
			public static void dropTable(String dropTableString) throws IOException{
				//DROP TABLE table_name;
				ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
				if(queryStringTokens.size() == 3) {
					String tableName = queryStringTokens.get(2);
					if(tableName.equalsIgnoreCase("davisbase_columns") || tableName.equalsIgnoreCase("davisbase_tables")){
						System.out.println("This table cannot be dropped\n");
						return;
					}
					if(bPlusTreeMap.get(tableName)!=null){
						BPlusTree tree = bPlusTreeMap.get(tableName);
						tree.root.processDropString(tableName, rfTbl, rfCol);
						//tree.root.close();
						bPlusTreeMap.remove(tableName);
						tree.root.file.delete();
					}else{
						System.out.println("Table '"+tableName+"' doesn't exist\n");
						return;
					}
					
				}
				else {
					System.out.println("You have an error in your SQL syntax\n");
				}	
			}
			/*
			Method to insert a row into a table
			insert into tableName (column list) values (column values);*/
			public static void parseInsert(String queryString) throws IOException{

				/*check if current page of tbl file has space, if so then continue
				otherwise add a new page to the file.
				add appropriate data to the root page of the tbl file.

				when actually inserting the data, increment number of records on that
				page, add record start position
				write the data according to the data type it has been defined with in the
				create table command, and this data is present in the columns table as text
				retrieve that by traversing the columns table

				*/

				String commandElements[] = queryString.trim().split(" ");
				if(commandElements.length!=6){
					System.out.println("You have an error in your SQL syntax\n");
				}else{
					String tableName = commandElements[2];
					String columnList[] = commandElements[3].substring(1,commandElements[3].length()-1).split(",");
					String columnValues[] = commandElements[5].substring(1,commandElements[5].length()-1).split(",");

					if(!columnList[0].equalsIgnoreCase("row_id") && columnList.length == columnValues.length){
						System.out.println("Field 'row_id' doesn't have a default value\n");
					}
					else if(columnList.length != columnValues.length){
						System.out.println("Column count doesn't match value count at row 1\n");	
					}
					else if(columnList.length == columnValues.length && !tableName.equalsIgnoreCase("")) 
					{
						if(bPlusTreeMap.get(tableName)!=null){
							BPlusTree tree = bPlusTreeMap.get(tableName);
							tree.root.processInsertQuery(tableName, columnList, columnValues, rfTbl, rfCol, tree);
						}else{
							System.out.println("Table '"+tableName+"' doesn't exist\n");
							return;
						}	
					} 
					else 
					{
						System.out.println("You have an error in your SQL syntax\n");
					}
				}
			}
			
			/**
			 *  Stub method for executing select query
			 *  @param queryString is a String of the user input
			 */
			public static void parseSelectQuery(String queryString) throws IOException{
				String commandElements[] = queryString.trim().split(" ");
				String tableName = commandElements[3];
				if(commandElements[1].equalsIgnoreCase("*") && commandElements.length == 4){
					//select all rows from given table
					if(bPlusTreeMap.get(tableName)!=null){
						BPlusTree tree = bPlusTreeMap.get(tableName);
						tree.root.processSelectQuery(tableName,rfCol);
					}else{
						System.out.println("Table '"+tableName+"' doesn't exist\n");
						return;
					}	
				}
			}
		}