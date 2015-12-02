package mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;



public class MySQLAccess {
  private Connection connect = null;
  private Statement statement = null;
  private PreparedStatement pStatement = null;
  private ResultSet resultSet = null;

  public void readDataBase() throws Exception {
    try {
    	String driver   = "com.mysql.jdbc.Driver";
        String url      = "jdbc:mysql://localhost/test";
        String usuario  = "gabriel";
        String password = "gabriel";

		// Se carga el driver de conexión de MySql
        Class.forName(driver);
		// Se define la conexión con la base de datos
		connect = DriverManager.getConnection(url, usuario, password); 
				//DriverManager.getConnection("jdbc:mysql://localhost/test?user=gabriel&password=gabriel");
		
		// Statements allow to issue SQL queries to the database
		statement = connect.createStatement();

		//Datos a pasar por el metodo
		String user    = "gabriel";
		String mail    = "gabriel.calle92@gmail.com";
		String web     = "www.escoladeltreball.org";
		String summary = "bla bla bla";
		String comment = "Comentarios del sitio";
		
		// Metodo para insertar un registro
		insertarRegistro(user, mail, web, summary, comment);
		
		// Result set get the result of the SQL query
		resultSet = statement.executeQuery("select * from test.comments");
		writeResultSet(resultSet);
		
    } catch (Exception e) {
      throw e;
    } finally {
      close();
    }

  }
 
  /* FUNCION PARA INSERTAR UN REGISTRO EN UNA BASE DE DATOS.
   * 
   * @param user	String
   * @param mail	String
   * @param web		String
   * @param summary	String
   * @param comment	String
   */
  private void insertarRegistro(String user, String mail, String web, String summary, String comment){
		String query = "insert into test.comments (myuser, email, webpage ,summary, datum, comments) values (?,?,?,?,?,?)";
		try{
			Date hoy = new java.util.Date();
			pStatement = connect.prepareStatement(query);
			pStatement.setString(1, user);
			pStatement.setString(2, mail);
			pStatement.setString(3, web);
			pStatement.setString(4, summary);
			pStatement.setDate  (5, new java.sql.Date(hoy.getTime()));
			pStatement.setString(6, comment);
			
			if (pStatement.executeUpdate() == 1){
				System.out.println("Se ha insertado el registro\n");	
			}
		}catch (SQLException e){
			System.err.println("ERROR: SQLException");
			e.printStackTrace();
		}
	}
  
  private void writeResultSet(ResultSet resultSet) throws SQLException {
    // ResultSet is initially before the first data set
    while (resultSet.next()) {
      // It is possible to get the columns via name
      // also possible to get the columns via the column number
      // which starts at 1
      // e.g. resultSet.getSTring(2);
      String user    = resultSet.getString("myuser");
      String website = resultSet.getString("webpage");
      String summary = resultSet.getString("summary");
      Date date      = resultSet.getDate("datum");
      String comment = resultSet.getString("comments");
      System.out.println("\tUser: "    + user);
      System.out.println("\tWebsite: " + website);
      System.out.println("\tSummary: " + summary);
      System.out.println("\tDate: "    + date);
      System.out.println("\tComment: " + comment);
      System.out.println("\t---------------------------");
    }
  }

  // You need to close the resultSet
  private void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }

      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }

} 