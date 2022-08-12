import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.ibatis.common.jdbc.ScriptRunner;


public class csvtodb {
	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws ClassNotFoundException,
		SQLException {
		
		String featurePath = "/home/imthath/Desktop/xmlutilspy/samples/belugga_feature_v1.csv";
		String mediaPath = "/home/imthath/Desktop/xmlutilspy/samples/belugga_media_v1.csv";
		String hotelPath = "/home/imthath/Desktop/xmlutilspy/samples/belugga_hotel_v1.csv";
		String roomTypePath = "/home/imthath/Desktop/xmlutilspy/samples/belugga_room_v1.csv";
		
		
		// Create MySql Connection
		Class.forName("org.postgresql.Driver");
//		Connection con = DriverManager.getConnection(
//			"jdbc:postgresql://localhost:5432/rezbase_v3_activity_setup", "postgres", "postgres");
		
		String connectionString = "jdbc:postgresql://10.15.11.121:5432/135_126";
		String connectionUsername = "postgres";
		String connectionPassword = "";

		
//		Statement stmt = null;
		long recordsImported[] = new long[]{-1};
//		String tableName = "ride.belugga_v1_feature_2022_07_01";
		
		//Insert hotel data
		
		Connection con = null;
		
				
		try {
			con = DriverManager.getConnection(
					connectionString, connectionUsername, connectionPassword);
			con.setAutoCommit (false);
						
			System.out.println("+++++++++START HOTEL :"+new Timestamp(System.currentTimeMillis())+"+++++++++++++++");
//			CopyManager copyManager = ((PGConnection) con).getCopyAPI();
			
			CopyManager copyManager = new CopyManager((BaseConnection) con);

		    FileReader fileReader = new FileReader(hotelPath);
		      
		    recordsImported[0] = copyManager.copyIn("COPY ride.belugga_hotel_v1_reference_2022_07_01 FROM STDIN DELIMITER ',' QUOTE '\"' CSV",fileReader);
		      
//		      recordsImported[0] = copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );
		    con.commit();
		    System.out.println("+++++++++END HOTEL :"+new Timestamp(System.currentTimeMillis())+" +++++++++++");

		} catch (Exception e) {
			con.rollback();
			System.err.println("Failed to Execute" + hotelPath
					+ " The error is " + e.getMessage());
		} finally {
			con.close();
		}
		
		//Insert feature of hotel data
		try {
			
			con = DriverManager.getConnection(
					connectionString, connectionUsername, connectionPassword);
			con.setAutoCommit (false);
			
			System.out.println("+++++++++START FEATURE : "+new Timestamp(System.currentTimeMillis())+"+++++++++++++++");
//			CopyManager copyManager = ((PGConnection) con).getCopyAPI();
			
			CopyManager copyManager = new CopyManager((BaseConnection) con);

		    FileReader fileReader = new FileReader(featurePath);
		      
//		    recordsImported[0] = copyManager.copyIn("COPY ride.belugga_v1_feature_2022_07_01 FROM STDIN DELIMITER ',' QUOTE '\"' CSV",fileReader);
		      
//		      recordsImported[0] = copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );
		    con.commit();
		    System.out.println("+++++++++END FEATURE : "+new Timestamp(System.currentTimeMillis())+"+++++++++++");

		} catch (Exception e) {
			con.rollback();
			System.err.println("Failed to Execute" + featurePath
					+ " The error is " + e.getMessage());
		} finally {
			con.close();
		}
		
		//Insert feature of media data
		try {
			
			con = DriverManager.getConnection(
					connectionString, connectionUsername, connectionPassword);
			con.setAutoCommit (false);
			
			System.out.println("+++++++++START MEDIA : "+new Timestamp(System.currentTimeMillis())+"+++++++++++++++");
//			CopyManager copyManager = ((PGConnection) con).getCopyAPI();
			
			CopyManager copyManager = new CopyManager((BaseConnection) con);

		    FileReader fileReader = new FileReader(mediaPath);
		      
//		    recordsImported[0] = copyManager.copyIn("COPY ride.belugga_v1_media_2022_07_01 FROM STDIN DELIMITER ',' QUOTE '\"' CSV",fileReader);
		      
//		      recordsImported[0] = copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );
		    con.commit();
		    System.out.println("+++++++++END MEDIA : "+new Timestamp(System.currentTimeMillis())+"+++++++++++");

		} catch (Exception e) {
			con.rollback();
			System.err.println("Failed to Execute" + mediaPath
					+ " The error is " + e.getMessage());
		} finally {
			con.close();
		}
		
		//Insert feature of room type data
		try {
			
			con = DriverManager.getConnection(
					connectionString, connectionUsername, connectionPassword);
			con.setAutoCommit (false);
			
			
			System.out.println("+++++++++START ROOM TYPE : "+new Timestamp(System.currentTimeMillis())+"+++++++++++++++");
//			CopyManager copyManager = ((PGConnection) con).getCopyAPI();
			
			CopyManager copyManager = new CopyManager((BaseConnection) con);

		    FileReader fileReader = new FileReader(roomTypePath);
		      
		    recordsImported[0] = copyManager.copyIn("COPY ride.belugga_v1_room_type_2022_07_01 FROM STDIN DELIMITER ',' QUOTE '\"' CSV",fileReader);
		      
//		      recordsImported[0] = copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );
		    con.commit();
		    System.out.println("+++++++++END ROOM TYPE : "+new Timestamp(System.currentTimeMillis())+"+++++++++++");

		} catch (Exception e) {
			con.rollback();
			System.err.println("Failed to Execute" + roomTypePath
					+ " The error is " + e.getMessage());
		} finally {
			con.close();
		}
		
	}
}
