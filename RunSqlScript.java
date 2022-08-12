import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.ibatis.common.jdbc.ScriptRunner;

/**
 * 
 @author Dhinakaran Pragasam
 */
public class RunSqlScript {
	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws ClassNotFoundException,
		SQLException {
		
		String aSQLScriptFilePath = "/home/imthath/Desktop/xmlutils.py/samples/belugga_feature_v1.sql";
		
		// Create MySql Connection
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection(
			"jdbc:postgresql://10.15.11.121:5432/135_126", "postgres", "");
		con.setAutoCommit (false);
		Statement stmt = null;

		try {
			// Initialize object for ScripRunner
			ScriptRunner sr = new ScriptRunner(con, false, false);

			// Give the input file to Reader
			Reader reader = new BufferedReader(
                               new FileReader(aSQLScriptFilePath));

			// Exctute script
			sr.runScript(reader);

		} catch (Exception e) {
			System.err.println("Failed to Execute" + aSQLScriptFilePath
					+ " The error is " + e.getMessage());
		}
		con.close();
	}
}
