/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to get the last reservation number in the database to
	 * ensure that each new reservation has a unique 
	 * 
	 * @return integer of the last reservation number
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int getReservationNumber () throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		String query = "SELECT MAX(rnum) FROM Reservation;";
		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);
		
		rs.next();
		String rnumStr = rs.getString(1);
		int rnum = Integer.parseInt(rnumStr);
		return rnum;
	}
	
	/**
	 * Method to check if the currently looked at flight is fully booked
	 * 
	 * @param flight ID
	 * @return true if flight is full, false if flight is not yet full
	 * @throws java.sql.SQLException when failed to execute the query
	 */ 
	public boolean isFlightFull(String flightID) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();
		
		//Get the plane flying
		String query = "SELECT plane_id FROM FlightInfo WHERE fiid = " + flightID + ";";
		//issue the query instruction and store result
		ResultSet rs = stmt.executeQuery (query);
		rs.next();
		String planeID = rs.getString(1);
		
		//Get number of seats on the plane
		query = "SELECT seats FROM Plane WHERE id = " + planeID + ";";
		//issue the query instruction and store result
		rs = stmt.executeQuery (query);
		
		rs.next();
		String seatsStr = rs.getString(1);
		int numOfSeatsOnPlane = Integer.parseInt(seatsStr); 
		
		//Get flight num_sold
		query = "SELECT num_sold FROM Flight WHERE fnum = " + flightID + ";";
		//issue the query instruction and store result
		rs = stmt.executeQuery (query);
		
		rs.next();
		String numSeatsSoldStr = rs.getString(1);
		int numSeatsSold = Integer.parseInt(numSeatsSoldStr);
		
		//System.out.print("Plane ID: " + planeID + "\n");
		//System.out.print("Seats Sold: " + numSeatsSold + "\n");
		//System.out.print("Total Seats: " + numOfSeatsOnPlane + "\n");
		//If number of seats >= num_sold return true, else increment num_sold and return false
		if(numSeatsSold >= numOfSeatsOnPlane) {
			System.out.print("Full" + "\n");
			return true;
		} else {
			//System.out.print("Not Full" + "\n");
			query = "UPDATE Flight SET num_sold = num_sold + 1 WHERE fnum = " + flightID + ";";
			//issue the query instruction then return false
			stmt.executeUpdate(query);
			return false;
		}
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1
		try{
			System.out.print("Plane ID (make sure this doesn't already exist): \n");
			String plane_input = in.readLine();
			System.out.print("Plane make: \n");
			String make = in.readLine();
			System.out.print("Plane model: \n");
			String model = in.readLine();
			System.out.print("Plane age: \n");
			String p_age = in.readLine();
			System.out.print("Plane number of seats: \n");
			String seat_num = in.readLine();
			// Done asking user for info

			int plane_int = Integer.parseInt(plane_input);
			
			String query = "INSERT INTO Plane(id,make,model,age,seats)\n"
						    +"VALUES(" 
							+ plane_int
							+ ", '" + make 
							+ "', '" + model
							+ "', " + p_age
							+ ", " + seat_num + " );";
			//System.out.print(query);
			esql.executeUpdate(query);
      	}catch(Exception e){
         	System.err.println (e.getMessage());
		}
	}

	public static void AddPilot(DBproject esql) {//2
		try{
			System.out.print("Pilot ID (make sure this doesn't already exist): \n");
			String pilot_input = in.readLine();
			System.out.print("Pilot fullname: \n");
			String fullname = in.readLine();
			System.out.print("Pilot Nationality: \n");
			String nationality = in.readLine();
			// Done asking user for info

			int pilot_id = Integer.parseInt(pilot_input);
			
			String query = "INSERT INTO Pilot\n"
						    +"VALUES(" 
							+ pilot_id
							+ ", '" + fullname 
							+ "', '" + nationality + "' );";
			//System.out.print(query);
			esql.executeUpdate(query);
      	}catch(Exception e){
         	System.err.println (e.getMessage());
		}
	}

	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB
		try{
			System.out.print("Flight num (make sure this doesn't already exist): \n");
			String flight_num = in.readLine(); // make int
			System.out.print("Flight cost: \n");
			String cost = in.readLine(); // make int
			System.out.print("Number sold: \n");
			String sold = in.readLine(); // make int
			System.out.print("Number of stops: \n");
			String stops = in.readLine(); // make int
			System.out.print("Departure year: \n");
			String dyear = in.readLine();
			System.out.print("Departure month (min. 2 digits): \n");
			String dmonth = in.readLine();
			System.out.print("Departure day (min. 2 digits): \n");
			String dday = in.readLine();
			System.out.print("Departure hour (min. 2 digits): \n");
			String dhour = in.readLine();
			System.out.print("Departure minutes (min. 2 digits): \n");
			String dmin = in.readLine();
			String depart_date = dyear + "-" + dmonth + "-" + dday +" " + dhour +":" +dmin;

			System.out.print("Arrival year: \n");
			String ayear = in.readLine();
			System.out.print("Arrival month (min. 2 digits): \n");
			String amonth = in.readLine();
			System.out.print("Arrival day (min. 2 digits): \n");
			String aday = in.readLine();
			System.out.print("Arrival hour (min. 2 digits): \n");
			String ahour = in.readLine();
			System.out.print("Arrival minutes (min. 2 digits): \n");
			String amin = in.readLine();
			String arrival_date = ayear + "-" + amonth + "-" + aday +" " + ahour +":" +amin;

			System.out.print("Arrival airport: \n");
			String arrival_airport = in.readLine();	
			System.out.print("Departure airport: \n");
			String dep_airport = in.readLine();	
			// Done asking user for info

			int fnum_int = Integer.parseInt(flight_num);
			int cost_int = Integer.parseInt(cost);
			int sold_int = Integer.parseInt(sold);
			int stops_int = Integer.parseInt(stops);
			
			String query = "INSERT INTO Flight\n"
						    +"VALUES(" 
							+ fnum_int
							+ ", " + cost_int 
							+ ", " + sold_int
							+ ", " + stops_int
							+ ", '" + depart_date
							+ "' ,'" + arrival_date
							+ "' ,'" + arrival_airport 
							+ "' ,'" + dep_airport +"');";
			//System.out.print(query);
			esql.executeUpdate(query);
			
			// Now to fill in flightInfo
			System.out.print("Flight Info ID (make sure this doesn't already exist): \n");
			String finfo_id = in.readLine(); // make int
			System.out.print("Pilot ID (Make sure it matches an existing pilot): \n");
			String pilot_id = in.readLine();
			System.out.print("Plane ID(Make sure it matches an existing plane ID): \n");
			String plane_id = in.readLine();
			String query2 = "INSERT INTO FlightInfo\n"
						    +"VALUES(" 
							+ finfo_id
							+ ", " + fnum_int
							+ ", " + pilot_id
							+ ", '" + plane_id +"');";
			esql.executeUpdate(query2);
      	}catch(Exception e){
         	System.err.println (e.getMessage());
		}
	}

	public static void AddTechnician(DBproject esql) {//4
			
		try{
			System.out.print("Technician ID (make sure this doesn't already exist): \n");
			String tech_input = in.readLine();
			System.out.print("Technician fullname: \n");
			String tech_fname = in.readLine();
			// Done asking user for info

			int tech_int = Integer.parseInt(tech_input);
			
			String query = "INSERT INTO Technician\n"
						    +"VALUES(" 
							+ tech_int
							+ ", '" + tech_fname + "' );";
			//System.out.print(query);
			esql.executeUpdate(query);
      	}catch(Exception e){
         	System.err.println (e.getMessage());
		}
	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
		try {
			System.out.print("customer id: \n");
			String customer_id = in.readLine();
			System.out.print("flight number: \n");
			String flight_num = in.readLine();
			System.out.print("Paid? (y/n): \n");
			String paid = in.readLine();
			//All info aquired
			//Get last reservation number and increment by 1
			int reservationNum = esql.getReservationNumber() + 1;
			
			//Check if flight is full
			boolean flightFull = esql.isFlightFull(flight_num);
			
			String status = "";
			
			if(flightFull) {
				status = "W";
			} else {
				switch(paid) {
					case "y":
						status = "C";
						break;
					default:
						status = "R";
						break;
				}
			}
			
			//System.out.print("RNUM: " + reservationNum + "\n");
			//System.out.print("Status: " + status + "\n");
			//Create query to make reservation
			String query = "INSERT INTO Reservation(rnum, cid, fid, status) \n" 
							+ "VALUES("
							+ reservationNum + ", "
							+ customer_id + ", "
							+ flight_num + ", "
							+ "'" + status + "'" + ");";
							
			esql.executeUpdate(query);
			System.out.print("Reservation (" + status + ") added with reservation #: " + reservationNum + "\n");
		} catch(Exception e) {
			System.err.println(e.getMessage());
		}
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
		try{
			System.out.print("Make sure all input exists in the database\n");
			System.out.print("Flight num: \n");
			String fnum_str = in.readLine();
			System.out.print("Flight year (atleast 4 digits): \n");
			String year = in.readLine();
			System.out.print("Flight month (atleast 2 digits): \n");
			String month = in.readLine();
			System.out.print("Flight day (atleast 2 digits): \n");
			String day = in.readLine();
			System.out.print("Flight hour (atleast 2 digits): \n");
			String hour = in.readLine();
			System.out.print("Flight minute (atleast 2 digits): \n");
			String min = in.readLine();
			// Done asking user for info
			
			String user_date = year + "-" + month + "-" + day + " " + hour + ":" +min;
			//System.out.println(user_date);
			
			int user_fnum = Integer.parseInt(fnum_str);
			String query = 	"SELECT P.seats - F.num_sold as seats_available, F.actual_departure_date as departure_date_and_time "
						+	"FROM Flight F, Plane P, FlightInfo FI "
						+	"WHERE P.id = FI.plane_id AND F.actual_departure_date = '" + user_date + "' "
						// P.id = Flightinfo Pid to find correct plane seats
						// Adding user date to find the date of the flight
						+   "AND F.fnum = FI.flight_id AND F.fnum = " + user_fnum 
						// They must equal each other bc the flightid references flight num
						+	" GROUP BY seats_available, F.actual_departure_date;" ;
		 esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
         	System.err.println (e.getMessage());
		}
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
		try {
			
			String query = "SELECT plane_id, COUNT(*) AS NumOfRepairs \n" 
						+ "FROM Repairs GROUP BY plane_id \n"
						+ "ORDER BY NumOfRepairs DESC;";
			
			esql.executeQueryAndPrintResult(query);
		} catch(Exception e) {
			System.err.println(e.getMessage());
		}
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
		try{
			//"SELECT EXTRACT(YEAR from R.repair_date) as year, COUNT(R.rid) "
		String query = "SELECT EXTRACT(YEAR from R.repair_date) as year, COUNT(R.rid) "
						+ "FROM Repairs R "
						+ "GROUP BY year "
						+ "ORDER BY year asc ;"; 
		
		 esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
         	System.err.println (e.getMessage());
		}
	}
	
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
		try {
			System.out.print("Flight ID: \n");
			String flightID = in.readLine();
			System.out.print("Select status (W/C/R): \n");
			String status = in.readLine();
			
			if(status.equals("W") || status.equals("C") || status.equals("R")) {
				String query = "SELECT COUNT(*) AS NumberOfPassengers FROM Reservation WHERE status = '" + status + "'"
						+ "AND fid = " + flightID + ";";
			
				esql.executeQueryAndPrintResult(query);
			} else {
				System.out.print("Invalid status \n");
			}
		} catch(Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
