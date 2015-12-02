package mysql;

/**
 * @author iaw26567249
 *
 */
public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args){
		try {
			// Se instancia la clase MySQLAccess
			MySQLAccess dao = new MySQLAccess();
			dao.readDataBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
