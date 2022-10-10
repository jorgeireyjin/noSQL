package demo.mongodb;

import java.net.UnknownHostException;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class Ejemplo {
	Mongo m;
	DB db;
	DBCollection coll;

	public static void main(String[] args) {
		Ejemplo ej = new Ejemplo();
		ej.setup();
		ej.insertaDatos();
		ej.imprimeDatos();
		
	}
	
	private void setup() {
		
		try {
			m = new Mongo("localhost", 27017);
			db = m.getDB("sampleDB");  // nombre de la BD
			
			// Leer la tabla "Gente"
			coll = db.getCollection("gente");
			
			// Borrar los elementos de la "tabla"
			DBCursor cur = coll.find();
			while ( cur.hasNext() ) {
				coll.remove( cur.next() );
			}
		
		} catch( UnknownHostException uhe) {
			uhe.printStackTrace();
		} catch ( MongoException me) {
			me.printStackTrace();
		}
	} // del setup
	

	/*
	 * Genera el "documento" ( registro ) a insertar
	 */
	private BasicDBObject makePersonDocument (int id, String nombre, String sexo) {
		BasicDBObject doc = new BasicDBObject();
		doc.put("ID", id);
		doc.put("NOMBRE", nombre);
		doc.put("SEXO", sexo);
		
		return doc;
	}
	
	private void insertaDatos() {
		// Crear un indice Unico ASC
		coll.ensureIndex( new BasicDBObject("id",1).append("unique", true) );
		coll.createIndex( new BasicDBObject("nombre", 1));
		
		// Insertar
		coll.insert( makePersonDocument ( 1234, "TIMMY TURNER", "HOMBRE" ) );
		coll.insert( makePersonDocument ( 1235, "COSMO", "HOMBRE" ) );
		coll.insert( makePersonDocument ( 1236, "WANDA", "MUJER" ) );
		
		System.out.println("Cantidad de Documentos insertados : " + coll.getCount() );
	}
	
	private void imprimeDatos() {
		// Recupera todos los elementos
		DBCursor cur = coll.find();
	    imprimir(cur , "..::Recupera todos los documentos::..");
	    
	    // Recupera solo un ojeto
	    cur = coll.find( new BasicDBObject("ID", 1235) );
	    imprimir(cur , "..::Elemento con ID 1235::..");
	    
	    // Recupera objetos con ID < 1236
	    cur = coll.find( new BasicDBObject().append("ID", new BasicDBObject("$lt", 1236 )) );
	    imprimir(cur , "..::Elemento con ID < 1236::..");

	    // Recupera objeto con ID < 5000 y que sea Mujer
	    cur = coll.find( new BasicDBObject()
	    				.append("ID", new BasicDBObject("$lte", 5000 ))
   	    				.append("SEXO", "MUJER") );
	    imprimir(cur , "..::Elemento con ID < 5000 y que sea MUJER:..");
	    

	    cur = coll.find( new BasicDBObject()
					.append("NOMBRE", Pattern.compile("^W.*?$", Pattern.CASE_INSENSITIVE )) ) 
					.sort( new BasicDBObject("NOMBRE", -1 ) );
	    imprimir(cur , "..::Elemento con NOMBRE LIKE B%s ORDER BY NOMBRE desc :..");

	    cur = coll.find( new BasicDBObject()
	    			.append("SEXO", "MUJER")) 
					.sort( new BasicDBObject("ID", -1 ) )
					.limit(2);
	    imprimir(cur , "..::Obtiene los top 2 mujeres ( por ID ) :..");
	    
	}

	private void imprimir(DBCursor cur, String titulo ) {
		System.out.println("\n\n<<<<<<<<< " + titulo + " >>>>>>>>>>>");
		while ( cur.hasNext() ) {
			System.out.println( cur.next().get("ID") + " - " + cur.curr().get("NOMBRE") + " - " + cur.curr().get("SEXO") );
		}
	}
}
