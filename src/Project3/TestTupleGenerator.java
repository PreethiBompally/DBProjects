package DBPRJ3;




/*****************************************************************************************
 * @file  TestTupleGenerator.java
 *
 * @author   Sadiq Charaniya, John Miller
 */

import static java.lang.System.out;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/*****************************************************************************************
 * This class tests the TupleGenerator on the Student Registration Database
 * defined in the Kifer, Bernstein and Lewis 2006 database textbook (see figure
 * 3.6). The primary keys (see figure 3.6) and foreign keys (see example 3.2.2)
 * are as given in the textbook.
 */
public class TestTupleGenerator {
	/*************************************************************************************
	 * The main method is the driver for TestGenerator.
	 * 
	 * @param args the command-line arguments
	 * @throws IOException 
	 * 
	 * @author Sandeep Reddy Guthireddy, Rohan Swapneel Intipalli
	 */
	public static void main(String[] args) throws IOException {
		
		int[] rowCounter = { 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 25000, 50000 };

		File timestamp = new File("timestamp.txt");
		FileWriter fw = new FileWriter("timestamp.txt");

		for (int i = 0; i < rowCounter.length; i++) {
			System.out.println("Running for tuple size: " + rowCounter[i]);
			var test = new TupleGeneratorImpl();
			long st;
			long et;
			double dur;
	
			test.addRelSchema("Actor", "actor_id first_name last_name last_update", "Integer String String ZonedDateTime");
	
			test.addRelSchema("Address", "address_id address address2 district city_id postal_code phone last_update", "Integer String String String Integer String String ZonedDateTime");
	
			test.addRelSchema("Category", "catgory_id name last_update", "Integer String ZonedDateTime");
	
			// test.addRelSchema("City", "city_id city country_id last_update", "Integer String Integer ZonedDateTime");
			test.addRelSchema("City", "city_id city country_id last_update", "Integer String Integer ZonedDateTime");

	
			test.addRelSchema("Country", "country_id country last_update", "Integer String ZonedDateTime" );
			test.addRelSchema("Customer", "customer_id store_id first_name last_name email address_id activebool create_date last_update active", "Integer Integer String String String Integer Boolean LocalDate ZonedDateTime Integer");
			
			
	
			test.addRelSchema("Film", "film_id title description release_year language_id original_language_id rental_duration rental_rate length replacement_cost rating last_update special_features fulltext", "Integer String String String Integer String String ZonedDateTime String[] ");
	
			test.addRelSchema("Film_category", "film_id category_id last_update", "Integer Integer ZonedDateTime");
	
			test.addRelSchema("Inventory", "inventory_id film_id store_id last_update", "Integer Integer Integer ZonedDateTime");
			test.addRelSchema("Language", "language_id name last_update", "Integer String ZonedDateTime" );
			
			

	
			var tables = new String[] { "actor", "address", "category", "city", "country", "customer", "film", "film_actor", "film_category", "inventory", "language" };
			
			var tups = new int[] { rowCounter[i], 1000, 2000, 50000, 5000 };
			
	
			var resultTest = test.generate(tups);
			
			Table Actor = new Table("actor", "actor_id first_name last_name last_update", "Integer String String ZonedDateTime", "id");
			Table Address = new Table("address", "address_id address address2 district city_id postal_code phone last_update", "Integer String String String Integer String String ZonedDateTime", "id");
			Table Category = new Table("category", "catgory_id name last_update", "Integer String ZonedDateTime", "crsCode");
			Table City = new Table("city", "city_id city country_id last_update", "Integer String Integer ZonedDateTime",
					"crsCode semester");
			Table Country = new Table("country", "country_id country last_update", "Integer String ZonedDateTime",
					"id");
			Table Customer = new Table("customer", "customer_id store_id first_name last_name email address_id activebool create_date last_update active", "Integer Integer String String String Integer Boolean LocalDate ZonedDateTime Integer", "id");
			Table Film= new Table("film", "film_id title description release_year language_id original_language_id rental_duration rental_rate length replacement_cost rating last_update special_features fulltext", "Integer String String String Integer String String ZonedDateTime String[] ", "id");
			Table Film_actor = new Table("film_actor", "actor_id film_id last_update", "Integer Integer ZonedDateTime", "crsCode");
			Table Film_category= new Table("film_category", "film_id category_id last_update", "Integer Integer ZonedDateTime", "id");
			Table Inventory= new Table("inventory", "inventory_id film_id store_id last_update", "Integer Integer Integer ZonedDateTime", "id");
			Table Language= new Table("language", "language_id name last_update", "Integer String ZonedDateTime", "crsCode");
			
			
			
			for (var j = 0; j < resultTest[0].length; j++) {
				int length = resultTest[0][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[0][j].length; k++) {
					col[k] = (resultTest[0][j][k]).toString();
				} 
				Comparable[] Actor = { col[0], col[1], col[2], col[3] };
				actor.insert(Actor);
			}

			for (var j = 0; j < resultTest[1].length; j++) {
				int length = resultTest[1][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[1][j].length; k++) {
					col[k] = (resultTest[1][j][k]).toString();
				} 
				Comparable[] Address = { col[0], col[1], col[2], col[3], col[4], col[5], col[6], col[7] };
				address.insert(Address);
			}

			for (var j = 0; j < resultTest[2].length; j++) {
				int length = resultTest[2][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[2][j].length; k++) {
					col[k] = (resultTest[2][j][k]).toString();
				} 
				Comparable[] Category = { col[0], col[1], col[2] };
				category.insert(Category);
			}

			for (var j = 0; j < resultTest[3].length; j++) {
				int length = resultTest[3][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[3][j].length; k++) {
					col[k] = (resultTest[3][j][k]).toString();
				} 
				Comparable[] City = { col[0], col[1], col[2], col[3] };
				city.insert(City);
			}

			for (var j = 0; j < resultTest[4].length; j++) {
				int length = resultTest[4][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[4][j].length; k++) {
					col[k] = (resultTest[4][j][k]).toString();
				} 
				Comparable[] Country = { col[0], col[1], col[2] };
				country.insert(Country);
			}
			
			
			for (var j = 0; j < resultTest[5].length; j++) {
				int length = resultTest[0][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[0][j].length; k++) {
					col[k] = (resultTest[0][j][k]).toString();
				} 
				Comparable[] Customer = { col[0], col[1], col[2], col[3], col[4], col[5], col[6], col[7], col[8], col[9] };
				customer.insert(Customer);
			}

			for (var j = 0; j < resultTest[6].length; j++) {
				int length = resultTest[1][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[1][j].length; k++) {
					col[k] = (resultTest[1][j][k]).toString();
				} 
				Comparable[] Film = { col[0], col[1], col[2], col[3], col[4], col[5], col[6], col[7], col[8], col[9], col[10], col[11], col[12], col[13] };
				film.insert(Film);
	

			for (var j = 0; j < resultTest[7].length; j++) {
				int length = resultTest[2][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[2][j].length; k++) {
					col[k] = (resultTest[2][j][k]).toString();
				} 
				Comparable[] Film_actor = { col[0], col[1], col[2] };
				film_actor.insert(Film_actor);
			}

			for (var j = 0; j < resultTest[8].length; j++) {
				int length = resultTest[3][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[3][j].length; k++) {
					col[k] = (resultTest[3][j][k]).toString();
				} 
				Comparable[] Film_category = { col[0], col[1], col[2] };
				film_category.insert(Film_category);
			}

			for (var j = 0; j < resultTest[9].length; j++) {
				int length = resultTest[4][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[4][j].length; k++) {
					col[k] = (resultTest[4][j][k]).toString();
				} 
				Comparable[] Inventory = { col[0], col[1], col[2], col[3] };
				inventory.insert(Inventory);
			}
			
			for (var j = 0; j < resultTest[10].length; j++) {
				int length = resultTest[0][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[0][j].length; k++) {
					col[k] = (resultTest[0][j][k]).toString();
				} 
				Comparable[] Language = { col[0], col[1], col[2] };
				language.insert(Language);
			}

			
			Random ran = new Random();
			st = System.nanoTime();
			
			// non-indexed select
			Table nonIndexedSelectTest = student.nonIndexSelect(new KeyType(String.valueOf(resultTest[0][ran.nextInt(tups[0])][0])));
			
			// indexed select
//			Table indexedSelectTest = student.select(new KeyType(String.valueOf(resultTest[0][ran.nextInt(tups[0])][0])));

			// non-indexed join
//			Table nonIndexedJoinTest = transcript.equi_join("studId", "id", student);
			
			//indexed join
//			Table indexedJoinTest = transcript.i_join("studId", "id", student);
			
			et = System.nanoTime();
			dur = (double) (et - st) / 1e6;
	
			fw.write(String.valueOf(dur));
			fw.write("\n");
			
			// Print the query results
//			nonIndexedSelectTest.print();
//			indexedSelectTest.print();
//			nonIndexedJoinTest.print();
//			indexedJoinTest.print();
			



			
			System.out.println("\t\t" + dur + " millisecs");
		}
		fw.close();
	} // main

} // TestTupleGenerator
