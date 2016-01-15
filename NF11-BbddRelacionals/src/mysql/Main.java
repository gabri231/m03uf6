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
			ExamenBBDD dao = new ExamenBBDD();
			dao.readDataBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
