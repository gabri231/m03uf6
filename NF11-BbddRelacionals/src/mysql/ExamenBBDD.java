package mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;



public class ExamenBBDD {
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
		
		// Result set get the result of the SQL query
		/*
		statement.execute("CREATE TABLE clientes (num_clie smallint NOT NULL, empresa character varying(20) NOT NULL, rep_clie smallint NOT NULL,limite_credito numeric(8,2));");
		statement.execute("CREATE TABLE oficinas (oficina smallint NOT NULL, ciudad character varying(15) NOT NULL, region character varying(10) NOT NULL, dir smallint, objetivo numeric(9,2), ventas numeric(9,2) NOT NULL);");
		statement.execute("CREATE TABLE pedidos (num_pedido integer NOT NULL, fecha_pedido date NOT NULL, clie smallint NOT NULL, rep smallint, fab character(3) NOT NULL, producto character(5) NOT NULL, cant smallint NOT NULL, importe numeric(7,2) NOT NULL);");
		statement.execute("CREATE TABLE productos (id_fab character(3) NOT NULL, id_producto character(5) NOT NULL, descripcion character varying(20) NOT NULL, precio numeric(7,2) NOT NULL, existencias integer NOT NULL);");
		statement.execute("CREATE TABLE repventas (num_empl smallint NOT NULL, nombre character varying(15) NOT NULL, edad smallint, oficina_rep smallint, titulo character varying(10), contrato date NOT NULL, director smallint, cuota numeric(8,2), ventas numeric(8,2) NOT NULL);");
		
		statement.execute("INSERT INTO clientes values (2111, 'JCP Inc.',	103,	50000.00);");
		statement.execute("INSERT INTO clientes values (2102, 'First Corp.',	101,	65000.00);");
		statement.execute("INSERT INTO clientes values (2103, 'Acme Mfg.',	105,	50000.00);");
		statement.execute("INSERT INTO clientes values (2123, 'Carter & Sons',	102,	40000.00);");
		statement.execute("INSERT INTO clientes values (2107, 'Ace International',	110,	35000.00);");
		statement.execute("INSERT INTO clientes values (2115, 'Smithson Corp.',	101,	20000.00);");
		statement.execute("INSERT INTO clientes values (2101, 'Jones Mfg.',	106,	65000.00);");
		statement.execute("INSERT INTO clientes values (2112, 'Zetacorp',	108,	50000.00);");
		statement.execute("INSERT INTO clientes values (2121, 'QMA Assoc.',	103,	45000.00);");
		statement.execute("INSERT INTO clientes values (2114, 'Orion Corp',	102,	20000.00);");
		statement.execute("INSERT INTO clientes values (2124, 'Peter Brothers',	107,	40000.00);");
		statement.execute("INSERT INTO clientes values (2108, 'Holm & Landis',	109,	55000.00);");
		statement.execute("INSERT INTO clientes values (2117, 'J.P. Sinclair',	106,	35000.00);");
		statement.execute("INSERT INTO clientes values (2122, 'Three-Way Lines',	105,	30000.00);");
		statement.execute("INSERT INTO clientes values (2120, 'Rico Enterprises',	102,	50000.00);");
		statement.execute("INSERT INTO clientes values (2106, 'Fred Lewis Corp.',	102,	65000.00);");
		statement.execute("INSERT INTO clientes values (2119, 'Solomon Inc.',	109,	25000.00);");
		statement.execute("INSERT INTO clientes values (2118, 'Midwest Systems',	108,	60000.00);");
		statement.execute("INSERT INTO clientes values (2113, 'Ian & Schmidt',	104,	20000.00);");
		statement.execute("INSERT INTO clientes values (2109, 'Chen Associates',	103,	25000.00);");
		statement.execute("INSERT INTO clientes values (2105, 'AAA Investments',	101,	45000.00);");
		
		statement.execute("INSERT INTO oficinas values(22, 'Denver', 'Oeste', 108, 300000.00, 186042.00);");
		statement.execute("INSERT INTO oficinas values(11, 'New York', 'Este', 106, 575000.00, 692637.00);");
		statement.execute("INSERT INTO oficinas values(12, 'Chicago', 'Este', 104, 800000.00, 735042.00);");
		statement.execute("INSERT INTO oficinas values(13, 'Atlanta', 'Este', 105, 350000.00, 367911.00);");
		statement.execute("INSERT INTO oficinas values(21, 'Los Angeles', 'Oeste', 108, 725000.00,	835915.00);");



		statement.execute("INSERT INTO pedidos values(112961, '1989-12-17', 2117, 106, 'rei', '2a44l', 7,	31500.00)");
		statement.execute("INSERT INTO pedidos values(113012, '1990-01-11', 2111, 105, 'aci', '41003', 35,	3745.00)");
		statement.execute("INSERT INTO pedidos values(112989, '1990-01-03', 2101, 106, 'fea', '114', 6,	1458.00)");
		statement.execute("INSERT INTO pedidos values(113051, '1990-02-10', 2118, 108, 'qsa', 'k47', 4,	1420.00)");
		statement.execute("INSERT INTO pedidos values(112968, '1989-10-12', 2102, 101, 'aci', '41004', 34,	3978.00)");
		statement.execute("INSERT INTO pedidos values(110036, '1990-01-30', 2107, 110, 'aci', '4100z', 9,	22500.00)");
		statement.execute("INSERT INTO pedidos values(113045, '1990-02-02', 2112, 108, 'rei', '2a44r', 10,	45000.00)");
		statement.execute("INSERT INTO pedidos values(112963, '1989-12-17', 2103, 105, 'aci', '41004', 28,	3276.00)");
		statement.execute("INSERT INTO pedidos values(113013, '1990-01-14', 2118, 108, 'bic', '41003', 1,	652.00)");
		statement.execute("INSERT INTO pedidos values(113058, '1990-02-23', 2108, 109, 'fea', '112', 10,	1480.00)");
		statement.execute("INSERT INTO pedidos values(112997, '1990-01-08', 2124, 107, 'bic', '41003', 1,	652.00)");
		statement.execute("INSERT INTO pedidos values(112983, '1989-12-27', 2103, 105, 'aci', '41004', 6,	702.00)");
		statement.execute("INSERT INTO pedidos values(113024, '1990-01-20', 2114, 108, 'qsa', 'xk47', 20,	7100.00)");
		statement.execute("INSERT INTO pedidos values(113062, '1990-02-24', 2124, 107, 'fea', '114', 10,	2430.00)");
		statement.execute("INSERT INTO pedidos values(112979, '1989-10-12', 2114, 102, 'aci', '4100z', 6,	15000.00)");
		statement.execute("INSERT INTO pedidos values(113027, '1990-01-22', 2103, 105, 'aci', '41002', 54,	4104.00)");
		statement.execute("INSERT INTO pedidos values(113007, '1990-01-08', 2112, 108, 'imm', '773c', 3,	2925.00)");
		statement.execute("INSERT INTO pedidos values(113069, '1990-03-02', 2109, 107, 'imm', '775c', 22,	31350.00)");
		statement.execute("INSERT INTO pedidos values(113034, '1990-01-29', 2107, 110, 'rei', '2a45c', 8,	632.00)");
		statement.execute("INSERT INTO pedidos values(112992, '1989-11-04', 2118, 108, 'aci', '41002', 10,	760.00)");
		statement.execute("INSERT INTO pedidos values(112975, '1989-12-12', 2111, 103, 'rei', '2a44g', 6,	2100.00)");
		statement.execute("INSERT INTO pedidos values(113055, '1990-02-15', 2108, 101, 'aci', '4100x', 6,	150.00)");
		statement.execute("INSERT INTO pedidos values(113048, '1990-02-10', 2120, 102, 'imm', '779c', 2,	3750.00)");
		statement.execute("INSERT INTO pedidos values(112993, '1989-01-04', 2106, 102, 'rei', '2a45c', 24,	1896.00)");
		statement.execute("INSERT INTO pedidos values(113065, '1990-02-27', 2106, 102, 'qsa', 'xk47', 6,	2130.00)");
		statement.execute("INSERT INTO pedidos values(113003, '1990-01-25', 2108, 109, 'imm', '779c', 3,	5625.00)");
		statement.execute("INSERT INTO pedidos values(113049, '1990-02-10', 2118, 108, 'qsa', 'xk47', 2,	776.00)");
		statement.execute("INSERT INTO pedidos values(112987, '1989-12-31', 2103, 105, 'aci', '4100y', 11,	27500.00)");
		statement.execute("INSERT INTO pedidos values(113057, '1990-02-18', 2111, 103, 'aci', '4100x', 24,	600.00)");
		statement.execute("INSERT INTO pedidos values(113042, '1990-02-02', 2113, 101, 'rei', '2a44r', 5,	22500.00)");


		statement.execute("INSERT INTO productos values('rei', '2a45c',	'Vastago Trinquete', 79.00, 210)");
		statement.execute("INSERT INTO productos values('aci', '4100y',	'Extractor',	2750.00, 25)");
		statement.execute("INSERT INTO productos values('qsa', 'xk47',	'Reductor',	355.00, 38)");
		statement.execute("INSERT INTO productos values('bic', '41672',	'Plate',	180.00, 0)");
		statement.execute("INSERT INTO productos values('imm', '779c',	'Riostra 2-Tm',	1875.00, 9)");
		statement.execute("INSERT INTO productos values('aci', '41003',	'Articulo Tipo 3',	107.00, 207)");
		statement.execute("INSERT INTO productos values('aci', '41004',	'Articulo Tipo 4',	117.00, 139)");
		statement.execute("INSERT INTO productos values('bic', '41003',	'Manivela',	652.00, 3)");
		statement.execute("INSERT INTO productos values('imm', '887p',	'Perno Riostra',	250.00, 24)");
		statement.execute("INSERT INTO productos values('qsa', 'xk48',	'Reductor',	134.00, 203)");
		statement.execute("INSERT INTO productos values('rei', '2a44l',	'Bisagra Izqda.',	4500.00, 12)");
		statement.execute("INSERT INTO productos values('fea', '112',	'Cubierta',	148.00, 115)");
		statement.execute("INSERT INTO productos values('imm', '887h',	'Soporte Riostra',	54.00, 223)");
		statement.execute("INSERT INTO productos values('bic', '41089',	'Reten',	225.00, 78)");
		statement.execute("INSERT INTO productos values('aci', '41001',	'Articulo Tipo 1',	55.00, 277)");
		statement.execute("INSERT INTO productos values('imm', '775c',	'Riostra 1-Tm',	1425.00, 5)");
		statement.execute("INSERT INTO productos values('aci', '4100z',	'Montador',	2500.00, 28)");
		statement.execute("INSERT INTO productos values('qsa', 'xk48a',	'Reductor',	117.00, 37)");
		statement.execute("INSERT INTO productos values('aci', '41002',	'Articulo Tipo 2',	76.00, 167)");
		statement.execute("INSERT INTO productos values('rei', '2a44r',	'Bisagra Dcha.',	4500.00, 12)");
		statement.execute("INSERT INTO productos values('imm', '773c',	'Riostra 1/2-Tm',	975.00,	28)");
		statement.execute("INSERT INTO productos values('aci', '4100x',	'Ajustador',	25.00, 37)");
		statement.execute("INSERT INTO productos values('fea', '114',	'Bancada Motor',	243.00, 15)");
		statement.execute("INSERT INTO productos values('imm', '887x',	'Retenedor Riostra',	475.00, 32)");
		statement.execute("INSERT INTO productos values('rei', '2a44g',	'Pasador Bisagra',	350.00, 14)");

		statement.execute("INSERT INTO repventas VALUES (105, 'Bill Adams',	37,	13,	'Rep Ventas', '1988-02-12',	104,	350000.00	, 367911.00)");
		statement.execute("INSERT INTO repventas VALUES (109, 'Mary Jones',	31,	11,	'Rep Ventas', '1989-10-12',	106,	300000.00	, 392725.00)");
		statement.execute("INSERT INTO repventas VALUES (102, 'Sue Smith',	48,	21,	'Rep Ventas', '1986-12-10',	108,	350000.00	, 474050.00)");
		statement.execute("INSERT INTO repventas VALUES (106, 'Sam Clark',	52,	11,	'VP Ventas',	'1988-06-14',	null, 275000.00	, 299912.00)");
		statement.execute("INSERT INTO repventas VALUES (104, 'Bob Smith',	33,	12,	'Dir Ventas', '1987-05-19',	106,	200000.00	, 142594.00)");
		statement.execute("INSERT INTO repventas VALUES (101, 'Dan Roberts', 45, 12,	'Rep Ventas', '1986-10-20',	104,	300000.00	, 305673.00)");
		statement.execute("INSERT INTO repventas VALUES (110, 'Tom Snyder',	41,	null,	'Rep Ventas', '1990-01-13',	101,	null, 	75985.00)");
		statement.execute("INSERT INTO repventas VALUES (108, 'Larry Fitch', 62, 21,	'Dir Ventas', '1989-10-12',	106,	350000.00	, 361865.00)");
		statement.execute("INSERT INTO repventas VALUES (103, 'Paul Cruz',	29,	12,	'Rep Ventas', '1987-03-01',	104,	275000.00	, 286775.00)");
		statement.execute("INSERT INTO repventas VALUES (107, 'Nancy Angelli',	49,	22,	'Rep Ventas', '1988-11-14',	108, 300000.00	, 186042.00)");
		*/
		System.out.println("***************************************************************************");
		System.out.println("***************************************************************************");
		// 1. Escriu un mètode consultaClient, que donat un identificador ens mostri el nombre de comandes que ha realitzat de cada producte i fabricant. (2 punts)
		
		String cliente = "2111";
		
		String consultaFabricantes = "select num_clie, empresa, count(pedidos.fab) as fab_total, fab from clientes,pedidos where pedidos.clie = clientes.num_clie and pedidos.clie ="+ cliente +" group by fab;";
		resultSet = statement.executeQuery(consultaFabricantes);
		mostrarPedidosPorFabricante(resultSet, cliente);
		
		String consultaPedidos = "select num_clie, empresa, count(pedidos.producto) as producto_total, producto from clientes,pedidos where pedidos.clie = clientes.num_clie and pedidos.clie ="+ cliente +" group by producto;";
		resultSet = statement.executeQuery(consultaPedidos);
		mostrarNumeroPedidosPorProducto(resultSet, cliente);
		

		
		
		System.out.println("***************************************************************************");
		System.out.println("***************************************************************************");
		// . Escriure un mètode que insereixi una nova comanda, s'ha de tenir present que ha d'existir el client, el representant de vendes, el producte i el fabricant, a més s'haurà d'actualitzar l'stock a la taula de productos.  (3 punts)
		System.out.println("Ejercicio2");
		
		
		
		
		
		System.out.println("***************************************************************************");
		System.out.println("***************************************************************************");
		//3.  Voldrem oferir descomptes o premis a aquells clients que es gasten més diners. Mostra un llistat amb el nom del client, el número de comandes i l'import total, ordenat primer per la suma dels imports, després pel nombre de comandes(3 punts)
		System.out.println("Ejercicio3");
		String consultaClientes = "select clie,empresa, count(*) as pedidos, sum(importe) total from pedidos, clientes where clie=num_clie group by clie order by total desc, pedidos desc;";
		resultSet = statement.executeQuery(consultaClientes);
		consultarClientesDescompte(resultSet);
		
		
		
		
		
		
		System.out.println("***************************************************************************");
		System.out.println("***************************************************************************");
		//4.  Escriu un mètode per donar de baixa, que elimini el representant de vendes que menys ha venut I que no sigui director. (2punt)
		System.out.println("Ejercicio4");
		
		String consultaRepVentasABorrar = "select num_empl, nombre, ventas from repventas where ventas in (select min(ventas) from repventas) and titulo != 'Dir Ventas';";
		resultSet = statement.executeQuery(consultaRepVentasABorrar);
		int repVentas = obtenerRepVentas(resultSet);
		
		System.out.println("Se ha borrado el empleado(que no es director): " + repVentas);
		String sentenciaBorrarEmpleado ="delete from repventas where num_empl = "+ repVentas +";";
		System.out.println(sentenciaBorrarEmpleado);
		statement.executeUpdate(sentenciaBorrarEmpleado); // EXECUTE UPDATE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
    } catch (Exception e) {
    	System.err.println("Se ha producido el siguiente error: " + e.toString());
		System.err.println("El error está en la linea: "
			+ e.getStackTrace()[0].getLineNumber());
    } finally {
      close();
    }

  }
 
 /*
 *	Consultar los clientes para ofrecer descuentos.
 * @param resultSet
 * @throws SQLException
 */
private void consultarClientesDescompte(ResultSet resultSet) throws SQLException {
	  System.out.println("LISTADO DE CLIENTES PARA OFRECER DESCUENTOS");
	  System.out.println("+------+-------------------+------------+----------+");
	  System.out.println("| clie | empresa           | pedidos    | total    |");
	  System.out.println("+------+-------------------+------------+----------+");
	    while (resultSet.next()) {
	      int num_clie    = resultSet.getInt("clie");
	      String empresa = resultSet.getString("empresa");
	      int pedidos = resultSet.getInt("pedidos");
	      int total = resultSet.getInt("total");
	      
	      System.out.println("|"+ num_clie + "\t|"+String.format("%1$-17s", empresa) +" | "+ "\t" + pedidos + "\t|" + total );
	    }
}

private int obtenerRepVentas(ResultSet resultSet) throws SQLException {
	  int empleado = 0;
	  while (resultSet.next()) {
	      int num_empl    = resultSet.getInt("num_empl");
	      int ventas = resultSet.getInt("ventas");
	      String nombre = resultSet.getString("nombre");
	      System.out.println("El número de empleado con menor ventas es: " + num_empl + "(" + nombre + ") con unas ventas de: " + ventas) ;
	      empleado = num_empl;
	    }
	  return empleado;
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
  
  private void mostrarPedidosPorFabricante(ResultSet resultSet, String cliente) throws SQLException {
	System.out.println("PEDIDOS POR FABRICANTE PARA EL CLIENTE: " + cliente);
    while (resultSet.next()) {
      int num_clie    = resultSet.getInt("num_clie");// .getString("num_clie");
      String empresa = resultSet.getString("empresa");
      int fab_total = resultSet.getInt("fab_total");
      String fab = resultSet.getString("fab");
      
      System.out.println("\t Cliente: "    + num_clie);
      System.out.println("\t Empresa Cliente: " + empresa);
      System.out.println("\t Fabricante: " + fab + "-> Número de pedidos con este fabricante: "+fab_total);
      
      System.out.println("\t---------------------------");
      
    }
  }
  private void mostrarNumeroPedidosPorProducto(ResultSet resultSet, String cliente) throws SQLException {
	    // ResultSet is initially before the first data set
		System.out.println("PEDIDOS POR POR PRODUCTO DEL CLIENTE: " + cliente);
	    while (resultSet.next()) {
	      int num_clie    = resultSet.getInt("num_clie");// .getString("num_clie");
	      String empresa = resultSet.getString("empresa");
	      int producto_total = resultSet.getInt("producto_total");
	      String producto = resultSet.getString("producto");
	      
	      System.out.println("\t Cliente: "    + num_clie);
	      System.out.println("\t Empresa Cliente: " + empresa);
	      System.out.println("\t Producto: " + producto + "-> Número de pedidos de este producto: "+producto_total);
	      
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