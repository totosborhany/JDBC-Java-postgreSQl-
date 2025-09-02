package sqlconnection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.Timestamp;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

class FileReader {
	String filepath;
	private String jdbcUrl;
	private String username;
	private String password;
	Connection connection;

	FileReader(String myfilepath) throws SQLException {
		filepath = myfilepath;
		jdbcUrl = getProperty("jdbcUrl");
		username = getProperty("username");
		password = getProperty("password");
		connection = DriverManager.getConnection(jdbcUrl, username, password);
	}

	public List<String> getPropFiles() {
		File readedFile = new File(filepath);
		String[] files = readedFile.list();
		if (files == null || files.length == 0) {
			System.out.println("Sorry couldn't find any files");
			System.exit(0);
		}

		List<String> propFiles = new ArrayList<>();

		for (String file : files) {
			if (file.endsWith(".properties")) {
				propFiles.add(file);
			}
		}

		if (propFiles.isEmpty()) {
			System.out.println("Sorry, no property files found");
			System.exit(0);
		}
		return propFiles;
	}

	public void createTableFromFiles() throws SQLException {
		if (this.connection == null) {
			System.out.println("Database connection is not initialized!");
			return;
		}
		List<String> propFiles = new ArrayList<>();
		propFiles = getPropFiles();
		String table_name;
		int pindex;
		String mychar = "=";
		String proppath = "C:\\temp\\javaProject\\";
		for (int i = 0; i < propFiles.size(); i++) {

			pindex = propFiles.get(i).indexOf(".properties");
			table_name = propFiles.get(i).substring(0, pindex);
			DatabaseMetaData md = connection.getMetaData();

			// Check if the table exists in the schema (replace "public" with your schema if
			// needed)
			ResultSet rs = md.getTables(null, "java", table_name, new String[] { "TABLE" });

			if (rs.next()) {
				// Table exists, now attempt to drop it
				String dropSQL = "DROP TABLE java." + table_name; // Update schema name if needed
				PreparedStatement dropStmt = connection.prepareStatement(dropSQL);
				dropStmt.executeUpdate();
				System.out.println("former table " + table_name + " has been deleted.");

			}
			StringBuilder creationStatement = new StringBuilder("create table ");
			creationStatement.append("java." + table_name + " (");
			String finalproppath =proppath + propFiles.get(i);
			
			String column_name = null, column_type = null;
			try (BufferedReader br = new BufferedReader(new java.io.FileReader(finalproppath))) {
				String line;
				List<String> brline = new ArrayList<>();
				while ((line = br.readLine()) != null) {
					brline.add(line);
				}
				for (int j = 0; j < brline.size(); j++) {
					// System.out.println(line);
					if ((brline.get(j).substring(1, brline.get(j).indexOf(mychar))).contains("column_name")) {
						column_name = brline.get(j).substring(brline.get(j).indexOf(mychar) + 1,
								brline.get(j).length());
						if (j == 0) {
							creationStatement.append(column_name + " ");
						} else {
							creationStatement.append("," + column_name + " ");

						}
					} else if ((brline.get(j).substring(1, brline.get(j).indexOf(mychar)).contains("column_type"))) {
						column_type = brline.get(j).substring(brline.get(j).indexOf(mychar) + 1,
								brline.get(j).length());
						if (column_type.contains("varchar")) {
							continue;
						} else {
							if (j == brline.size() - 1) {
								creationStatement.append(column_type + ");");
							} else {

								creationStatement.append(column_type);
							}

						}

					} else if ((brline.get(j).substring(1, brline.get(j).indexOf("=")).contains("column_size"))) {
						String column_size = brline.get(j).substring(brline.get(j).indexOf(mychar) + 1,
								brline.get(j).length());
						column_type = "varchar(" + column_size + ")";
						if (j == brline.size() - 1) {
							creationStatement.append(column_type + ");");
						} else {

							creationStatement.append(column_type);
						}

					}

				}
				String finalStatement = creationStatement.toString();
				PreparedStatement mystatement = connection.prepareStatement(finalStatement);
				mystatement.executeUpdate();
				System.out.println("table " + table_name + " has been created");

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	private static String getProperty(String key) {
		Properties databaseInfo = new Properties();
		try (InputStream is = new FileInputStream("DataBaseConfig.properties");) {
			databaseInfo.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return databaseInfo.getProperty(key);

	}

};

class SqlFileWriter {
	String file_name;
	private String jdbcUrl;
	private String username;
	private String password;
	Connection connection;

	SqlFileWriter(String myfile) throws SQLException, FileNotFoundException {

		file_name = myfile;
		jdbcUrl = getProperty("jdbcUrl");
		username = getProperty("username");
		password = getProperty("password");
		connection = DriverManager.getConnection(jdbcUrl, username, password);

	}

	private static String getProperty(String key) {
		Properties databaseInfo = new Properties();

		try (InputStream is = new FileInputStream("DataBaseConfig.properties");) {
			databaseInfo.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return databaseInfo.getProperty(key);

	}

	/**
	 * must call this method to close the connection
	 * 
	 * @throws SQLException
	 */
	public void closeConnection() throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}

	}

//TODO:coding styles in java
	public void createFile() {
		File csvfile = new File(file_name);
	}

	/**
	 * takes sql query and writes it to a csv file format
	 * 
	 * @param myquerey
	 * @throws IOException
	 * @throws SQLException
	 */
	public void writeQueryToFile(String selectQuery) throws IOException, SQLException {
		File csvfile = new File(file_name);
		PrintWriter out = new PrintWriter(csvfile);
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(selectQuery);
		ResultSetMetaData rsMetaData = rs.getMetaData();
		ArrayList<String> columns = new ArrayList<>();

		int count = rsMetaData.getColumnCount();
		for (int i = 1; i <= count; i++) {
			columns.add(escape(rsMetaData.getColumnName(i)));
		}
		out.println(String.join(",", columns));

		while (rs.next()) {
			List<String> row = new ArrayList<>();
			for (int i = 1; i <= count; i++) {
				Object value = rs.getObject(i);
				row.add(value.toString());
			}
			out.println(String.join(",", row));
		}

		out.close();
		rs.close();
		closeConnection();
		LocalDate currentDate = LocalDate.now();


	}

	/**
	 * , (comma) → Required because CSV uses commas as field separators. " (double
	 * quotes) → Must be escaped by doubling them (""). \n (new line) → Newlines
	 * must be enclosed in quotes for CSV formatting.
	 * 
	 * @param value
	 * @return
	 */
	private String escape(String value) {
		if (value == null)
			return "";
		if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
			value = value.replace("\"", "\"\"");
			return "\"" + value + "\"";
		}
		return value;
	}

};

class ReadCsvFiletoSql {
	String file_name;
	Connection connection;
	private String jdbcUrl;
	private String username;
	private String password;

	ReadCsvFiletoSql(String myfile) throws SQLException, FileNotFoundException {
		jdbcUrl = getProperty("jdbcUrl");
		username = getProperty("username");
		password = getProperty("password");
		file_name = myfile;
		connection = DriverManager.getConnection(jdbcUrl, username, password);

	}

	/**
	 * it returns the jdbcurl username and password of the data base
	 * 
	 * @param key
	 * @return
	 */
	private static String getProperty(String key) {
		Properties databaseInfo = new Properties();
		try (InputStream is = new FileInputStream("DataBaseConfig.properties");) {
			databaseInfo.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return databaseInfo.getProperty(key);

	}

	public void closeConnection() throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}

	}

	public void createTable(String table_name) throws SQLException, IOException {
		Properties table_info = new Properties();
		String filepath = "C:\\temp\\Assigments\\" + file_name;
		BufferedReader fileReader = new BufferedReader(new java.io.FileReader(filepath));

		String[] columnNames = null;
		ArrayList<String> columnTypes = new ArrayList<String>();
		String[] sampleRow = null;
		if (fileReader.lines() != null) {
			columnNames = fileReader.readLine().split(",");
		}

		if (fileReader.lines() != null) {
			sampleRow = fileReader.readLine().split(",");
		}
		if (fileReader.lines() != null) {
			sampleRow = fileReader.readLine().split(",");
		}
		List<String> numbers = new ArrayList<>();
		numbers.add("1");
		numbers.add("2");
		numbers.add("3");
		numbers.add("4");
		numbers.add("5");
		numbers.add("6");
		numbers.add("7");
		numbers.add("8");
		numbers.add("9");

		int column_number = columnNames.length;
		if (column_number < 0) {
			System.out.println("column numbers are less than 1 sorry you cant conntionu");
			System.exit(0);
		}
		// instanceof
		for (int i = 0; i < column_number; i++) {

			if (numbers.contains(sampleRow[i].substring(0, 1))) {

				if (sampleRow[i].contains("/")) {
					columnTypes.add("date");

				} else {
					columnTypes.add("numeric");
				}
			}

			else {
				columnTypes.add("varchar(100)");
			}
		}

		Properties tableInfo = new Properties();
		OutputStream os = new FileOutputStream("TableInfo.properties");
		String tblnum = "", tbltype = "";
		for (int i = 0; i < column_number; i++) {
			tblnum = "column_name" + Integer.toString(i + 1);
			tbltype = "column_type" + Integer.toString(i + 1);
			String Sval = columnNames[i];
			tableInfo.setProperty(tblnum, columnNames[i]);
			String val = columnTypes.get(i);
			tableInfo.setProperty(tbltype, val);

		}
		tableInfo.store(os, null);
		InputStream is = new FileInputStream("TableInfo.properties");
		tableInfo.load(is);
		StringBuilder tableCreationStatement = new StringBuilder("Create table " + table_name + " (");
		for (int i = 0; i < column_number; i++) {
			tblnum = "column_name" + Integer.toString(i + 1);
			tbltype = "column_type" + Integer.toString(i + 1);
			if (i == 0) {
				tableCreationStatement.append(tableInfo.getProperty(tblnum) + " " + tableInfo.getProperty(tbltype));
			} else if (i == (column_number - 1)) {
				tableCreationStatement
						.append("," + tableInfo.getProperty(tblnum) + " " + tableInfo.getProperty(tbltype) + ");");
			} else {
				tableCreationStatement
						.append("," + tableInfo.getProperty(tblnum) + " " + tableInfo.getProperty(tbltype));

			}
		}
		String finalStatment = tableCreationStatement.toString();
		PreparedStatement tableCreation = connection.prepareStatement(finalStatment);
		tableCreation.executeUpdate();
		System.out.println("Table " + table_name + " created successfully !!!!");
	}

	/**
	 * takes a csv file turns to sql table
	 * 
	 * @param table_name
	 * @throws IOException
	 * @throws SQLException
	 * @throws ParseException
	 */
	public void readCsvToTable(String table_name) throws IOException, SQLException, ParseException {
		String table_statment = "SELECT * FROM " + table_name;
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(table_statment);
		ResultSetMetaData rsMetaData = rs.getMetaData();
		DatabaseMetaData md = connection.getMetaData();

		if (md.allTablesAreSelectable()) {
			System.out.println(table_name + " already exists ");
		} else {
			createTable(table_name);
		}

		String filepath = "C:\\temp\\Assigments\\" + file_name;
		BufferedReader lines = new BufferedReader(new java.io.FileReader(filepath));
		String linetext = lines.readLine();

		String[] columns = null;
		if (linetext != null) {
			columns = linetext.split(",");

		}
		int c = columns.length;

		StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + table_name + " VALUES(");
		for (int i = 0; i < c; i++) {
			statementBuilder.append("?");
			if (i < c - 1) {
				statementBuilder.append(",");
			}
		}
		statementBuilder.append(")");
		String statement = statementBuilder.toString();

		PreparedStatement insertionStatment = connection.prepareStatement(statement);

		while ((linetext = lines.readLine()) != null) {
			String[] rows = linetext.split(",");

			for (int i = 0; i < c; i++) {
				if (rsMetaData.getColumnTypeName(i + 1).toLowerCase().contains("int")) {
					insertionStatment.setInt(i + 1, Integer.parseInt(rows[i]));
				} else if (rsMetaData.getColumnTypeName(i + 1).toLowerCase().contains("date")) {
					/**
					 * simple date format it's a class responsible to give from string to a date
					 * returning date/time formatter
					 * 
					 * 
					 * java.sql.Date filterss the time out to give a pure date takes milliseconds
					 *
					 */

					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
					java.util.Date utilDate = sdf.parse(rows[i]);

					java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());

					insertionStatment.setDate(i + 1, sqlDate);
				} else if (rsMetaData.getColumnTypeName(i + 1).toLowerCase().contains("numeric")) {

					insertionStatment.setFloat(i + 1, Float.parseFloat(rows[i]));

				}

				else {
					insertionStatment.setString(i + 1, rows[i]);
				}
			}
			insertionStatment.executeUpdate();
		}
		closeConnection();
		LocalDate currentDate = LocalDate.now();


	}

};

/**
 * 
 * @author ahmed
 *
 */
public class TestSql {
// TODO: PUT THIS CLASS IN A SEPRATE CLASS 
	/**
	 * 
	 * @author ahmed
	 *
	 */

	// TODO: PUT THIS CLASS IN A SEPRATE CLASS

	/**
	 * 
	 * @param myfile
	 * @throws SQLException
	 * @throws FileNotFoundException
	 */

	public static void main(String[] args) throws Exception {
//		String querey = "SELECT * FROM public.customer";
//		SqlFileWriter mycollege = new SqlFileWriter("customer.csv");
//        mycollege.writeQueryToFile(querey);
		String myfilepath = "C:\\temp\\javaProject";
		FileReader readf = new FileReader(myfilepath);
		System.out.println(readf.getPropFiles());
		readf.createTableFromFiles();
		// ReadCsvFiletoSql readcollege = new ReadCsvFiletoSql("mycollge.csv");
		// readcollege.createTable("ahmed.mymarvelmovies3");
//		readcollege.readCsvToTable("ahmed.collegestuff");

	}
}
