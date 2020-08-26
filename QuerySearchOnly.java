import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;
import java.util.*;

/**
 * Runs queries against a back-end database.
 * This class is responsible for searching for flights.
 */
public class QuerySearchOnly
{
  // `dbconn.properties` config file
  private String configFilename;
  //List of itineraries, added for hw8.
  public Queue<Itinerary> itineraryList;

  // DB Connection
  protected Connection conn;



  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  protected PreparedStatement checkFlightCapacityStatement;
  
  private static final String CHECK_DIRECT_FLIGHT = "SELECT TOP (?) fid,day_of_month,carrier_id,flight_num,origin_city,dest_city,"
                                                    + "actual_time,capacity,price FROM Flights WHERE origin_city = ? AND dest_city = ?"
                                                    + " AND day_of_month = ? AND canceled = 0 ORDER BY actual_time, fid ASC";
  protected PreparedStatement checkDirectFlight;
  
  private static final String NUM_DIRECT_FLIGHT = "SELECT COUNT(*) AS cnt FROM Flights WHERE origin_city = ? AND dest_city = ?"
                                                + " AND day_of_month = ? AND canceled = 0";
  protected PreparedStatement numDirectFlight;
  
  private static final String CHECK_INDIRECT_FLIGHT = 
        "SELECT TOP (?) x1.fid AS fid1,x1.day_of_month AS day1,x1.carrier_id AS id1,x1.flight_num AS num1,x1.origin_city AS ori1, "
        + "x1.dest_city AS dest1,x1.actual_time AS time1,x1.capacity AS cap1,x1.price AS price1, " 
        + "x2.fid AS fid2,x2.day_of_month AS day2,x2.carrier_id AS id2,x2.flight_num AS num2,x2.origin_city AS ori2, "
        + "x2.dest_city AS dest2,x2.actual_time AS time2,x2.capacity AS cap2,x2.price AS price2 "
        + "FROM Flights AS x1, Flights AS x2 "
        + "WHERE (x1.origin_city = ? AND x2.dest_city = ? " 
        + "AND x1.dest_city = x2.origin_city AND x1.day_of_month = x2.day_of_month AND x1.day_of_month = ? "
        + "AND x1.canceled = 0 AND x2.canceled = 0) "
        + "ORDER BY x1.actual_time + x2.actual_time, x1.fid, x2.fid ASC";
  protected PreparedStatement checkIndirectFlight;
  
  // Added for HW8
  private static final String GET_NUM_RESERV = "SELECT COUNT(*) AS cnt FROM Reservation WHERE fid1 = ? OR fid2 = ?";
  protected PreparedStatement getNumReserv;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    public Flight(int fid, int dayOfMonth, String carrierId, String flightNum,
      String originCity, String destCity, int time, int capacity, int price) {
        this.fid = fid;
        this.dayOfMonth = dayOfMonth;
        this.carrierId = carrierId;
        this.flightNum = flightNum;
        this.originCity = originCity;
        this.destCity = destCity;
        this.time = time;
        this.capacity = capacity;
        this.price = price;
    }

    public Flight(int time) {
      this.fid = -1;
      this.time = time;
    }

    public int getTime() {
      return this.time;
    }

    public int getFid() {
      return this.fid;
    }
    //Added for HW8
    public int getCapacity() {
      return this.capacity;
    }

    public int getDay() {
      return this.dayOfMonth;
    }

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public QuerySearchOnly(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /** Open a connection to SQL Server in Microsoft Azure.  */
  public void openConnection() throws Exception
  {
    Properties configProps = new Properties();
    configProps.load(new FileInputStream(configFilename));

    String jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    String jSQLUrl = configProps.getProperty("flightservice.url");
    String jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    String jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true);
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    //by default automatically commit after each statement
    /* In the full Query class, you will also want to appropriately set the transaction's isolation level:
          conn.setTransactionIsolation(...)
       See Connection class's JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    checkIndirectFlight = conn.prepareStatement(CHECK_INDIRECT_FLIGHT);
    checkDirectFlight = conn.prepareStatement(CHECK_DIRECT_FLIGHT);
    numDirectFlight = conn.prepareStatement(NUM_DIRECT_FLIGHT);
    getNumReserv = conn.prepareStatement(GET_NUM_RESERV);
    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
  }



  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise it searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    String result = "";
    // Please implement your own (safe) version that uses prepared statements rather than string concatenation.
    // You may use the `Flight` class (defined above).
    try {  
      int num = countDirectFlights(originCity, destinationCity, dayOfMonth);
      if (directFlight || num >= numberOfItineraries) {
          result = checkDirectFlights(numberOfItineraries, directFlightResults(numberOfItineraries, originCity, destinationCity, dayOfMonth));
      } else {
          result = checkIndirectFlights(numberOfItineraries, num, originCity, destinationCity, dayOfMonth,
                                        directFlightResults(numberOfItineraries, originCity, destinationCity, dayOfMonth));
      }
    } catch (SQLException e) {
       return "Failed to search\n";
    }
    return result; 
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();

    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: ").append(result_dayOfMonth)
                .append(" Carrier: ").append(result_carrierId)
                .append(" Number: ").append(result_flightNum)
                .append(" Origin: ").append(result_originCity)
                .append(" Destination: ").append(result_destCity)
                .append(" Duration: ").append(result_time)
                .append(" Capacity: ").append(result_capacity)
                .append(" Price: ").append(result_price)
                .append('\n');
      }
      oneHopResults.close();
    } catch (SQLException e) { e.printStackTrace(); }

    return sb.toString();
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments.
   * You don't need to use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  private int countDirectFlights(String origin_city, String dest_city, int day_of_month) throws SQLException
  {
    numDirectFlight.clearParameters();
    numDirectFlight.setString(1, origin_city);
    numDirectFlight.setString(2, dest_city);
    numDirectFlight.setInt(3, day_of_month);
    ResultSet results = numDirectFlight.executeQuery();
    results.next();
    int result = results.getInt("cnt");
    results.close();

    return result;
  }

  private ResultSet directFlightResults(int numberOfItineraries, String origin_city, String dest_city, int day_of_month) throws SQLException
  {
    checkDirectFlight.clearParameters();
    checkDirectFlight.setInt(1, numberOfItineraries);
    checkDirectFlight.setString(2, origin_city);
    checkDirectFlight.setString(3, dest_city);
    checkDirectFlight.setInt(4, day_of_month);
    ResultSet results = checkDirectFlight.executeQuery();

    return results;
  }

  private String checkDirectFlights(int numberOfItineraries, ResultSet results) throws SQLException
  {
    itineraryList = new LinkedList<>();
    StringBuffer sb = new StringBuffer();
    int i = 0;
    while (results.next() && i < numberOfItineraries) {    
       Flight f = new Flight(results.getInt("fid"), results.getInt("day_of_month"), results.getString("carrier_id"),
          results.getString("flight_num"), results.getString("origin_city"),results.getString("dest_city"),
          results.getInt("actual_time"), results.getInt("capacity"), results.getInt("price"));
        
        sb.append("Itinerary ").append(i)
          .append(": 1 flight(s), ")
          .append(results.getInt("actual_time")).append(" minutes").append('\n')
          .append(f.toString())
          .append('\n');
          i++;
          itineraryList.add(new Itinerary(f));
    }
    results.close();
    if (sb.toString().equals("")) {
      return "No flights match your selection\n";
    }
    return sb.toString();
  }

  private String checkIndirectFlights(int numberOfItineraries, int num, String origin_city, String dest_city,
                                      int day_of_month, ResultSet directResults) throws SQLException
  {
    itineraryList = new LinkedList<>();
    StringBuffer sb2 = new StringBuffer();
    int i = 0;
    checkIndirectFlight.clearParameters();
    checkIndirectFlight.setInt(1, numberOfItineraries - num);
    checkIndirectFlight.setString(2, origin_city);
    checkIndirectFlight.setString(3, dest_city);
    checkIndirectFlight.setInt(4, day_of_month);
    ResultSet results = checkIndirectFlight.executeQuery();

    Queue<Itinerary> direct = new LinkedList<>();
    Queue<Itinerary> indirect = new LinkedList<>();

    while(directResults.next()) {


      Flight f = new Flight(directResults.getInt("fid"), directResults.getInt("day_of_month"), directResults.getString("carrier_id"),
          directResults.getString("flight_num"), directResults.getString("origin_city"),directResults.getString("dest_city"),
          directResults.getInt("actual_time"), directResults.getInt("capacity"), directResults.getInt("price"));
      Itinerary i1 = new Itinerary(f);
      direct.add(i1);
    }

    while(results.next()) {

      Flight f1 = new Flight(results.getInt("fid1"),results.getInt("day1"),results.getString("id1"),
                   results.getString("num1"),results.getString("ori1"),results.getString("dest1"),
                   results.getInt("time1"),results.getInt("cap1"),results.getInt("price1"));
      Flight f2 = new Flight(results.getInt("fid2"),results.getInt("day2"),results.getString("id2"),
                   results.getString("num2"),results.getString("ori2"),results.getString("dest2"),
                   results.getInt("time2"),results.getInt("cap2"),results.getInt("price2"));
      Itinerary i2 = new Itinerary(f1, f2);
      indirect.add(i2);
    }

    //these two parameters are for testing purpose only.
    //int size1 = direct.size();
    //int size2 = indirect.size();

    while (direct.size() != 0 && indirect.size() != 0) {
      Itinerary d = direct.peek();
      Itinerary id = indirect.peek();
      if (d.compareTo(id) < 0) {
        Itinerary it = direct.remove();
        sb2.append("Itinerary ").append(i)
             .append(": 1 flight(s), ")
             .append(it.time()).append(" minutes").append('\n')
             .append(it.first().toString())
             .append('\n');
             i++;
             itineraryList.add(it);
      } else if (d.compareTo(id) > 0) {
        Itinerary it2 = indirect.remove();
        sb2.append("Itinerary ").append(i)
          .append(": 2 flight(s), ")
          .append(it2.time()).append(" minutes").append('\n')
          .append(it2.first().toString())
          .append('\n')
          .append(it2.second().toString())
          .append('\n');
          i++;
          itineraryList.add(it2);
      } else if (d.breaktie(id) < 0) {
          Itinerary it = direct.remove();
          Itinerary it2 = indirect.remove();
          sb2.append("Itinerary ").append(i)
             .append(": 1 flight(s), ")
             .append(it.time()).append(" minutes").append('\n')
             .append(it.first().toString())
             .append('\n')
             .append("Itinerary ").append(i+1)
             .append(": 2 flight(s), ")
             .append(it2.time()).append(" minutes").append('\n')
             .append(it2.first().toString())
             .append('\n')
             .append(it2.second().toString())
             .append('\n');
          i += 2;
          itineraryList.add(it);
          itineraryList.add(it2);
      } else {
        Itinerary it = direct.remove();
        Itinerary it2 = indirect.remove();
        sb2.append("Itinerary ").append(i)
          .append(": 2 flight(s), ")
          .append(it2.time()).append(" minutes").append('\n')
          .append(it2.first().toString())
          .append('\n')
          .append(it2.second().toString())
          .append('\n')
          .append("Itinerary ").append(i)
          .append(": 1 flight(s), ")
          .append(it.time()).append(" minutes").append('\n')
          .append(it.first().toString())
          .append('\n');
        i += 2;
        itineraryList.add(it2);
        itineraryList.add(it);
      } 
    }

    //if there are more direct flights
    if (direct.size() != 0) {
        int size = direct.size();
        for (int j = 0; j < Math.min(numberOfItineraries, size); j++) {
          Itinerary it3 = direct.remove();
          sb2.append("Itinerary ").append(i)
               .append(": 1 flight(s), ")
               .append(it3.time()).append(" minutes").append('\n')
               .append(it3.first().toString())
               .append('\n');
          i++;
          itineraryList.add(it3);
        }
    }
    //if there are more one-hop flights
    if (indirect.size() != 0) {
       int size = indirect.size();
       for (int j = 0; j < Math.min(numberOfItineraries, size); j++) {
          Itinerary it4 = indirect.remove();
          sb2.append("Itinerary ").append(i)
            .append(": 2 flight(s), ")
            .append(it4.time()).append(" minutes").append('\n')
            .append(it4.first().toString())
            .append('\n')
            .append(it4.second().toString())
            .append('\n');
            i++;
            itineraryList.add(it4);
       }
    }

    results.close();
    if (sb2.toString().equals("")) {
       return "No flights match your selection\n";
    }

    return sb2.toString();
    //for testing purpose only.
    /*return "the size of indirect: " + size2 + "\n" +
    "the size of direct: " + size1 + "\n" + "after running the size of indirect: " + indirect.size() + "\n"
    + "the size of direct: " + direct.size();*/
  }

  class Itinerary{
    public Flight firstFlight;
    public Flight secondFight;

    public Itinerary() {
      this.firstFlight = null;
      this.secondFight = null;
    }

    public Itinerary(Flight firstFlight) {
      this.firstFlight = firstFlight;
      this.secondFight = new Flight(0);
    }

    public Itinerary(Flight firstFlight,Flight secondFight) {
      this.firstFlight = firstFlight;
      this.secondFight = secondFight;
    }

    public int time() {
        return this.firstFlight.getTime() + this.secondFight.getTime();
    }

    public int compareTo(Itinerary i1) {
      return this.time() - i1.time();
    }

    public Flight first() {
      return this.firstFlight;
    }

    public Flight second() {
      return this.secondFight;
    }

    public int firstCapacity() {
      return this.firstFlight.getCapacity();
    }

    public int secondCapacity() {
      return this.secondFight.getCapacity();
    }

    public int[] getFid() {
      int[] result = new int[2]; 
      result[0] = this.firstFlight.getFid();
      result[1] = this.secondFight.getFid();
      return result;
    }

    public int getDay() {
      return this.firstFlight.getDay();
    }

    public int breaktie(Itinerary i1) {
      int firstID = this.firstFlight.getFid();
      int secondID = this.secondFight.getFid();
      int foreignFID = i1.firstFlight.getFid();
      int foreignSID = i1.secondFight.getFid();
      if (firstID != foreignFID) {
        return firstID - foreignFID;
      } 
      return secondID - foreignSID;
    }
  }
}
