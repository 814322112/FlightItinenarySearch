import java.sql.*;
import java.util.*;

public class Query extends QuerySearchOnly {

	// Logged In User
	private String username; // customer username is unique
	private int rid;
	private int timeOfRetry;

	// transactions
	private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	protected PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	protected PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	protected PreparedStatement rollbackTransactionStatement;

	private static final String DELETE_USERS = "DELETE FROM Users";
	protected PreparedStatement deleteUsersStatement;

	private static final String DELETE_RESERVATIONS = "DELETE FROM Reservation";
	protected PreparedStatement deleteReservationsStatement;

	private static final String CREATE_ACCOUNT = "INSERT INTO Users VALUES(?,?,?)";
	protected PreparedStatement createAccountStatement;

	private static final String CHECK_PSWD_CORRECT = "SELECT * FROM Users WHERE username = ? AND password = ?";
	protected PreparedStatement checkPswdStatement;

	private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
	protected PreparedStatement checkFlightCapacityStatement;

	private static final String GET_ITINERARIES_BASED_ON_DAY = "SELECT * FROM Reservation AS r" +
									   " WHERE r.username = ? AND r.dayOfMonth = ? AND canceled = 0";
    protected PreparedStatement getItiBasedOnDay;

    private static final String UPDATE_RESERVATIONS = "INSERT INTO Reservation VALUES(?,?,0,?,?,?,0,?)";
    protected PreparedStatement updateReservationsStatement;

    private static final String GET_MAX_RID = "SELECT MAX(rid) AS cnt FROM Reservation";
    protected PreparedStatement getMaxRidStatement;

    private static final String GET_RESERVED_INFO = "SELECT rid, paid, fid1, fid2 FROM Reservation WHERE username = ? AND canceled = 0";
    protected PreparedStatement getReservedInfo;

    private static final String GET_FLIGHT = "SELECT * FROM Flights WHERE fid = ?";
    protected PreparedStatement getFlight;

    private static final String GET_BALANCE = "SELECT r.paid AS paid, u.balance AS balance FROM Users AS u, Reservation AS r "
    										+ "WHERE r.username = u.username AND r.rid = ? AND r.canceled = 0";
    protected PreparedStatement getBalance;

    private static final String GET_PRICE = "SELECT price FROM Flights WHERE fid = ?";
    protected PreparedStatement getPrice;

    private static final String GET_FID = "SELECT fid1, fid2 FROM Reservation WHERE rid = ? AND canceled = 0";
    protected PreparedStatement getFid;

    private static final String GET_TOTAL = "SELECT price FROM Reservation WHERE rid = ?";
    protected PreparedStatement getTotal;

    private static final String UPDATE_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
    protected PreparedStatement updateBalance;

    private static final String UPDATE_PAID = "UPDATE Reservation SET paid = 1 WHERE rid = ?";
    protected PreparedStatement updatePaid;

    private static final String CHECK_USER_RESERVE = "SELECT * FROM Reservation WHERE canceled = 0 AND rid = ? AND username = ?";
    protected PreparedStatement chkUserRes;

    private static final String CANCEL_RESERVATION = "UPDATE Reservation SET canceled = 1 WHERE rid = ?";
    protected PreparedStatement cancelRes;

    private static final String GET_CANCEL_STATUS = "SELECT canceled FROM Reservation WHERE rid = ?";
    protected PreparedStatement getCanSta;

	public Query(String configFilename) {
		super(configFilename);
	}


	/**
	 * Clear the data in any custom tables created. Do not drop any tables and do not
	 * clear the flights table. You should clear any tables you use to store reservations
	 * and reset the next reservation ID to be 1.
	 */
	public void clearTables ()
	{
		try {
			rid = 1;
			beginTransaction();
			deleteReservationsStatement.executeUpdate();
			deleteUsersStatement.executeUpdate();
			commitTransaction();
		} catch (SQLException e) {
			try {
		   		rollbackTransaction();
		   		//e.printStackTrace();
			} catch (SQLException e2) {
				//e.printStackTrace();
			}
		}
	}


	/**
	 * prepare all the SQL statements in this method.
	 * "preparing" a statement is almost like compiling it.
	 * Note that the parameters (with ?) are still not filled in
	 */
	@Override
	public void prepareStatements() throws Exception
	{
		super.prepareStatements();
		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
		createAccountStatement = conn.prepareStatement(CREATE_ACCOUNT);
		deleteUsersStatement = conn.prepareStatement(DELETE_USERS);
		deleteReservationsStatement = conn.prepareStatement(DELETE_RESERVATIONS);
		checkPswdStatement = conn.prepareStatement(CHECK_PSWD_CORRECT);
		checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
		getItiBasedOnDay = conn.prepareStatement(GET_ITINERARIES_BASED_ON_DAY);
		updateReservationsStatement = conn.prepareStatement(UPDATE_RESERVATIONS);
		getMaxRidStatement = conn.prepareStatement(GET_MAX_RID);
		getReservedInfo = conn.prepareStatement(GET_RESERVED_INFO);
		getFlight = conn.prepareStatement(GET_FLIGHT);
		getBalance = conn.prepareStatement(GET_BALANCE);
		getPrice = conn.prepareStatement(GET_PRICE);
		getFid = conn.prepareStatement(GET_FID);
		getTotal = conn.prepareStatement(GET_TOTAL);
		updateBalance = conn.prepareStatement(UPDATE_BALANCE);
		updatePaid = conn.prepareStatement(UPDATE_PAID);
		chkUserRes = conn.prepareStatement(CHECK_USER_RESERVE);
		cancelRes = conn.prepareStatement(CANCEL_RESERVATION);
		getCanSta = conn.prepareStatement(GET_CANCEL_STATUS);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
	}


	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @return If someone has already logged in, then return "User already logged in\n"
	 * For all other errors, return "Login failed\n".
	 *
	 * Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password)
	{
		try {
			beginTransaction();
			if (this.username != null) {
				rollbackTransaction();
				return "User already logged in\n";
			}			
			checkPswdStatement.clearParameters();
			checkPswdStatement.setString(1, username);
			checkPswdStatement.setString(2, password);
			ResultSet results = checkPswdStatement.executeQuery();
			if (!results.next()) {
				rollbackTransaction();
				return "Login failed\n";
			}
			results.close();
			this.username = username;
			commitTransaction();
			itineraryList = new LinkedList<>();
		} catch (SQLException e) {
			try {
		   		rollbackTransaction();
		    	return "Login failed\n";
			} catch (SQLException e2) {
				return "Login failed\n";
			}
		}
		return "Logged in as " + username + "\n";
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username new user's username. User names are unique the system.
	 * @param password new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
	 *
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 */
	public String transaction_createCustomer (String username, String password, int initAmount)
	{
		try {
		   beginTransaction();
		   if (initAmount < 0) {
		      rollbackTransaction();
		      return "Failed to create user\n";
		   } else {		   	   
		       createAccountStatement.clearParameters();
		       createAccountStatement.setString(1, username);
		       createAccountStatement.setString(2, password);
		       createAccountStatement.setInt(3, initAmount);
		       createAccountStatement.executeUpdate();
		       commitTransaction();
		   } 
	    } catch (SQLException e) {
		   	try {
		   		rollbackTransaction();
		    	return "Failed to create user\n";
			} catch (SQLException e2) {
				return "Failed to create user\n";
			}
		}
	    return "Created user " + username + "\n";
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
	 *
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
	 * If the user already has a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n".
	 * For all other errors, return "Booking failed\n".
	 *
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
	 * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
	 * successful reservation is made by any user in the system.
	 */
	public String transaction_book(int itineraryId)
	{
		try {
			beginTransaction();
			if (this.username == null) {
				rollbackTransaction();
				return "Cannot book reservations, not logged in\n";
			}
			//invalid itineraryID
			int size = itineraryList.size();
			//return "size = " + size;

			if (itineraryId >= size) {
				rollbackTransaction();
				return "No such itinerary " + itineraryId + "\n";
			}

			//get selected itinerary
			Itinerary iti = new Itinerary();
			for (int i = 0; i <= itineraryId; i++) {
				iti = itineraryList.remove();
			}

			//check flight capacity
			int[] fids = iti.getFid();
			getNumReserv.clearParameters();
			getNumReserv.setInt(1,fids[0]);
			getNumReserv.setInt(2,fids[0]);
			ResultSet seatsBooked = getNumReserv.executeQuery();
			seatsBooked.next();
			int booked1 = seatsBooked.getInt("cnt");
			seatsBooked.close();
			int booked2 = 0;

			if(fids[1] != -1) {
				getNumReserv.clearParameters();
				getNumReserv.setInt(1,fids[1]);
				getNumReserv.setInt(2,fids[1]);
				ResultSet seatsBooked2 = getNumReserv.executeQuery();
				seatsBooked2.next();
				booked2 = seatsBooked2.getInt("cnt");	
				seatsBooked2.close();
			}

			//if this is a direct flight
			if (fids[1] == -1) {
				if (iti.firstCapacity() - booked1 == 0) {
					rollbackTransaction();
					return "Booking failed\n";
				}
			} else { //or if this is a one-hop flight
				if (iti.firstCapacity() - booked1 == 0 || iti.secondCapacity() - booked2 == 0) {
					rollbackTransaction();
					return "Booking failed\n";
				}
			}

			//check that if the user books two itineraries on the same day.
			int day = iti.getDay();
			getItiBasedOnDay.clearParameters();
			getItiBasedOnDay.setString(1, this.username);
			getItiBasedOnDay.setInt(2, day);
			ResultSet results = getItiBasedOnDay.executeQuery();
			if (results.next()) {
				rollbackTransaction();
				return "You cannot book two flights in the same day\n";
			}
			results.close();

			//find the new rid for this reservation
			ResultSet max = getMaxRidStatement.executeQuery();
			max.next();
			rid = max.getInt("cnt") + 1;
			max.close();

			//find and update the total price of this reservation
			getPrice.clearParameters();
			getPrice.setInt(1, fids[0]);
			ResultSet price1 = getPrice.executeQuery();
			price1.next();
			int priceA = price1.getInt("price");
			price1.close();

			int priceB = 0;
			if (fids[1] != -1) {
				getPrice.clearParameters();
				getPrice.setInt(1, fids[1]);
				ResultSet price2 = getPrice.executeQuery();
				price2.next();
				priceB = price2.getInt("price");
				price2.close();
			}

			int totalPrice = priceA + priceB;

			//update the new reservation
			updateReservationsStatement.clearParameters();
			updateReservationsStatement.setInt(1,rid);
			updateReservationsStatement.setInt(2, day);
			updateReservationsStatement.setString(3, this.username);
			updateReservationsStatement.setInt(4,fids[0]);
			updateReservationsStatement.setInt(5,fids[1]);
			updateReservationsStatement.setInt(6, totalPrice);
			updateReservationsStatement.executeUpdate();

			commitTransaction();

		} catch (SQLException e) {
			try {
		   		rollbackTransaction();
		   		/*if (timeOfRetry < 5) {
		   			timeOfRetry++;
		   			return transaction_book(itineraryId);
		   		} else {*/
					e.printStackTrace();
		    		return "Booking failed\n";
		    	//}
			} catch (SQLException e2) {
				//e.printStackTrace();
				return "Booking failed\n";
			}
		}
		return "Booked flight(s), reservation ID: " + rid + "\n";
	}

	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 *
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n"
	 * If the reservation is not found / not under the logged in user's name, then return
	 * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
	 * If the user does not have enough money in their account, then return
	 * "User has only [balance] in account but itinerary costs [cost]\n"
	 * For all other errors, return "Failed to pay for reservation [reservationId]\n"
	 *
	 * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
	 * where [balance] is the remaining balance in the user's account.
	 */
	public String transaction_pay (int reservationId)
	{
		//initialization of parameters.
		int balance = -1;
		int paid = -1;

		try{
			beginTransaction();

			if (this.username == null) {
				rollbackTransaction();
				return "Cannot pay, not logged in\n";
			}
			//get balance and check if the reservation is already paid.			
			getBalance.clearParameters();
			getBalance.setInt(1, reservationId);
			ResultSet bal = getBalance.executeQuery();
			if (bal.next()) {
				balance = bal.getInt("balance");
				paid = bal.getInt("paid");
			}
			bal.close();

			//If the reservation is already paid or no such reservation found under the username logged in.
			if (paid == 1 || paid == -1) {
				rollbackTransaction();
				return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
			}

			getTotal.clearParameters();
			getTotal.setInt(1, reservationId);
			ResultSet price = getTotal.executeQuery();
			price.next();
			int totalPrice = price.getInt("price");
			price.close();

			if (balance < totalPrice) {
				rollbackTransaction();
				return "User has only " + balance + " in account but itinerary costs " + totalPrice + "\n";
			}

			//deduct balance
			balance -= totalPrice;
			updateBalance.clearParameters();
			updateBalance.setInt(1, balance);
			updateBalance.setString(2, this.username);
			updateBalance.executeUpdate();

			//update payment status
			updatePaid.clearParameters();
			updatePaid.setInt(1,reservationId);
			updatePaid.executeUpdate();

			commitTransaction();

		} catch (SQLException e) {
			try {
		   		rollbackTransaction();
		    	//e.printStackTrace();
		    	return "Failed to pay for reservation: " + reservationId + "\n";
			} catch (SQLException e2) {
				//e.printStackTrace();
				return "Failed to pay for reservation: " + reservationId + "\n";
			}
		}
		return "Paid reservation: " + reservationId + " remaining balance: " + balance + "\n";
	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
	 * If the user has no reservations, then return "No reservations found\n"
	 * For all other errors, return "Failed to retrieve reservations\n"
	 *
	 * Otherwise return the reservations in the following format:
	 *
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * ...
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 *
	 * @see Flight#toString()
	 */
	public String transaction_reservations()
	{
		StringBuffer sb3 = new StringBuffer();
		try{
			beginTransaction();
			if (this.username == null) {
				rollbackTransaction();
				return "Cannot view reservations, not logged in\n";
			}
			
			getReservedInfo.clearParameters();
			getReservedInfo.setString(1, this.username);
			ResultSet results = getReservedInfo.executeQuery();

			while(results.next()) {
				int rid = results.getInt("rid");
				int paid = results.getInt("paid");
				String ifPaid = (paid == 1) ? "true" : "false";
				int fid1 = results.getInt("fid1");
				int fid2 = results.getInt("fid2");

				getFlight.clearParameters();
				getFlight.setInt(1,fid1);
				ResultSet flight1 = getFlight.executeQuery();
				flight1.next();
				Flight fli1 = new Flight(flight1.getInt("fid"),flight1.getInt("day_of_month"),flight1.getString("carrier_id"),
										flight1.getString("flight_num"),flight1.getString("origin_city"),flight1.getString("dest_city"),
										flight1.getInt("actual_time"),flight1.getInt("capacity"),flight1.getInt("price"));

				sb3.append("Reservation " + rid + " paid: " + ifPaid + ":\n")
				   .append(fli1.toString()).append("\n");

				flight1.close();
				if (fid2 != -1) {
					getFlight.clearParameters();
					getFlight.setInt(1,fid2);
					ResultSet flight2 = getFlight.executeQuery();
					flight2.next();
					Flight fli2 = new Flight(flight2.getInt("fid"),flight2.getInt("day_of_month"),flight2.getString("carrier_id"),
										flight2.getString("flight_num"),flight2.getString("origin_city"),flight2.getString("dest_city"),
										flight2.getInt("actual_time"),flight2.getInt("capacity"),flight2.getInt("price"));
					flight2.close();
					sb3.append(fli2.toString()).append("\n");
				}
			}
			results.close();
			if (sb3.toString().equals("")) {
				rollbackTransaction();				
				return "No reservations found\n";
			}
			commitTransaction();
		} catch (SQLException e) {
			try {
		   		rollbackTransaction();
		    	//e.printStackTrace();
		    	return "Failed to retrieve reservations\n";
			} catch (SQLException e2) {
				//e.printStackTrace();
				return "Failed to retrieve reservations\n";
			}
		}
		return sb3.toString();
	}

	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 *
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
	 * For all other errors, return "Failed to cancel reservation [reservationId]"
	 *
	 * If successful, return "Canceled reservation [reservationId]"
	 *
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId)
	{	
		//intializing the parameters.
		int paid = -1;
		int totalPrice = -1;
		int balance = -1;
		int canceled = -1;

		try {
			beginTransaction();

			if (username == null) {
				rollbackTransaction();
				return "Cannot cancel reservations, not logged in\n";
			}

			getCanSta.clearParameters();
			getCanSta.setInt(1, reservationId);
			ResultSet can = getCanSta.executeQuery();
			can.next();
			canceled = can.getInt("canceled");

			chkUserRes.clearParameters();
			chkUserRes.setInt(1, reservationId);
			chkUserRes.setString(2, this.username);
			ResultSet results = chkUserRes.executeQuery();
			
			if (!results.next() || canceled == 1 || canceled == -1) {
				rollbackTransaction();
				return "Failed to cancel reservation " + reservationId + "\n";
			}

			paid = results.getInt("paid");

			if (paid == 1) {
				
				getTotal.clearParameters();
				getTotal.setInt(1, reservationId);
				ResultSet price = getTotal.executeQuery();
				price.next();
				totalPrice = price.getInt("price");
				price.close();

				//find current balance
				getBalance.clearParameters();
				getBalance.setInt(1, reservationId);
				ResultSet bal = getBalance.executeQuery();
				if (bal.next()) {
					balance = bal.getInt("balance");
				}
				bal.close();

				//issue a refund
				balance += totalPrice;
				updateBalance.clearParameters();
				updateBalance.setInt(1, balance);
				updateBalance.setString(2, this.username);
				updateBalance.executeUpdate();

				//update payment status
				updatePaid.clearParameters();
				updatePaid.setInt(1,reservationId);
				updatePaid.executeUpdate();			
			} 

			//cancel the reservation
			cancelRes.clearParameters();
			cancelRes.setInt(1, reservationId);
			cancelRes.executeUpdate();

			commitTransaction();

		} catch (SQLException e) {
			try {
		   		rollbackTransaction();
		    	//e.printStackTrace();
		    	return "Failed to cancel reservation " + reservationId + "\n";
			} catch (SQLException e2) {
				//e.printStackTrace();
				return "Failed to cancel reservation " + reservationId + "\n";
			}
		}
		
		return "Canceled reservation " + reservationId  +"\n";
	}


	/* some utility functions below */

	public void beginTransaction() throws SQLException
	{
		conn.setAutoCommit(false);
		beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException
	{
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException
	{
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}
}
