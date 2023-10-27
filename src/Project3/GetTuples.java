package Project3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.time.ZonedDateTime;
import java.time.LocalDate;

import static java.lang.System.out;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;
//
public class GetTuples {
	private int counter = 0;

    /** Initializations 
     */
    private HashMap <String, Comparable [][]> result = new HashMap <> ();

    private HashMap <Integer, String> tableIndex = new HashMap <> ();

    private HashMap <String, String []> tableAttr = new HashMap <> ();

    private HashMap <String, String []> tableDomain = new HashMap <> ();

    private HashMap <String, String []> tablepks = new HashMap <> ();

    HashMap <String, String [][]> tablefks = new HashMap <> ();
    
    public void addRelSchema (String name, String attribute, String domain,
            String primaryKey, String [][] foreignKey)
	{
		tableIndex.put (counter, name);
        tableAttr.put (name, attribute.split (" "));
        tableDomain.put (name, domain.split (" "));
        tablepks.put (name, primaryKey.split (" "));
        tablefks.put (name, foreignKey);
        counter++;
	 // addRelSchema
	}
    public static void main(String args[]) throws IOException {
    	int[] rowCounter = { 100,250,500,750,1000,2500,5000,7500,10000,17000};

    	String file_path = Paths.get("").toAbsolutePath().toString()+"\\src\\Project3\\timestamp.txt";
		File timestamp = new File(file_path);
		FileWriter fw = new FileWriter(file_path);
    	
    	var test = new GetTuples();
    	
    	test.addRelSchema("actor","actor_id first_name last_name last_update","Integer String String String","actor_id",new String[][] {  });
    	test.addRelSchema("customer","customer_id store_id first_name last_name email address_id activebool create_date last_update active","Integer Integer String String String Integer Integer String String Integer","customer_id",new String[][] { {"String_id","String","String_id"},{"store_id","store","store_id"}});
    	test.addRelSchema("film","film_id title description release_year language_id original_language_id rental_duration rental_rate length replacement_cost rating last_update special_features fulltxt","Integer String String Integer Integer Integer Integer Float Integer Float String String String String","film_id",new String[][] { {"language_id","language","language_id"},{"original_language_id","language","language_id"}});
    	test.addRelSchema("film_actor","actor_id film_id last_update","Integer Integer String","actor_id film_id",new String[][] { {"actor_id","actor","actor_id"},{"film_id","film","film_id"}});
    	test.addRelSchema("rental","rental_id rental_date inventory_id customer_id return_date staff_id last_update","Integer String Integer Integer String Integer String","rental_id",new String[][] { {"customer_id","customer","customer_id"},{"inventory_id","inventory","inventory_id"},{"staff_id","staff","staff_id"}});
    	test.addRelSchema("inventory","inventory_id film_id store_id last_update","Integer Integer Integer String","inventory_id",new String[][] { {"film_id","film","film_id"},{"store_id","store","store_id"}});
    	
    	for (int i = 0; i < rowCounter.length; i++) {
			System.out.println("Running for tuple size: " + rowCounter[i]);
			long start_time;
			long end_time;
			double duration;
			String operation;
			var tables = new String[] { "actor", "customer", "film", "film_actor", "rental" };

			Table actor = new Table("actor","actor_id first_name last_name last_update","Integer String String String","actor_id");
			Table customer = new Table("customer","customer_id store_id first_name last_name email address_id activebool create_date last_update active","Integer Integer String String String Integer Integer String String Integer","customer_id");
			Table film = new Table("film","film_id title description release_year language_id original_language_id rental_duration rental_rate length replacement_cost rating last_update special_features fulltxt","Integer String String Integer Integer Integer Integer Float Integer Float String String String String","film_id");
			Table film_actor = new Table("film_actor","actor_id film_id last_update","Integer Integer String","actor_id film_id");
			Table rental = new Table("rental","rental_id rental_date inventory_id customer_id return_date staff_id last_update","Integer String Integer Integer String Integer String","rental_id");
			Table inventory = new Table("inventory","inventory_id film_id store_id last_update","Integer Integer String Integer","inventory_id");
			
			GetData data = new GetData();
			List<Comparable[]> tups = data.getTuples("actor",rowCounter[i]);
			for(Comparable[] tup:tups){
				actor.insert(tup);
			}
			List<Comparable[]> tups1 = data.getTuples("customer",rowCounter[i]);
			for(Comparable[] tup:tups1){
				customer.insert(tup);
			}
			List<Comparable[]> tups2 = data.getTuples("film",rowCounter[i]);
			for(Comparable[] tup:tups2){
				film.insert(tup);
			}
			List<Comparable[]> tups3 = data.getTuples("film_actor",rowCounter[i]);
			for(Comparable[] tup:tups3){
				film_actor.insert(tup);
			}
			List<Comparable[]> tups4 = data.getTuples("rental",rowCounter[i]);
			for(Comparable[] tup:tups4){
				rental.insert(tup);
			}
			List<Comparable[]> tups5 = data.getTuples("inventory",rowCounter[i]);
			for(Comparable[] tup:tups5){
				inventory.insert(tup);
			}
			
			Random ran = new Random();
			start_time = System.nanoTime();

//			non-indexed join
			out.println ();
			operation = "non-indexed-join for "+rowCounter[i]+" rows : ";
//			Table t_ijoin1 = film.join("film_id","film_id" , film_actor);//film,film_actor
//			
//			Table t_ijoin2 = film.i_join("film_id","film_id" , inventory); //film,rental
//			Table t_ijoin3 = t_ijoin2.i_join("inventory_id", "inventory_id", rental);
//			
//			Table t_ijoin4 = film.join("film_id","film_id" , film_actor);//film,film_actor,actor
//			Table t_ijoin5 = t_ijoin4.join("actor_id","actor_id",actor);
			
//			Table t_ijoin6 = film.join("film_id","film_id" , inventory); //film,rental,customer
//			Table t_ijoin7 = t_ijoin6.join("inventory_id", "inventory_id", rental);
//			Table t_ijoin8 = t_ijoin7.join("customer_id", "customer_id", customer);
			

			
//			var t_ijoin9 = film.select("film_id == 141");// select
//			t_ijoin9.print();
			
			var t_ijoin10 = film.select(new KeyType(141));
			t_ijoin10.print();
			
			
			
//			indexed join
//			operation = "indexed-join for "+rowCounter[i]+" rows : "; // film,film_actor
//			Table t_ijoin9 = film.i_join("film_id","film_id" , film_actor);
//			

//			Table t_ijoin10 = film.i_join("film_id","film_id" , inventory); //film,rental
//			Table t_ijoin11 = t_ijoin10.i_join("inventory_id", "inventory_id", rental);
			
			
//			Table t_ijoin12 = film.i_join("film_id","film_id" , film_actor);//film,film_actor,actor
//			Table t_ijoin13 = t_ijoin12.i_join("actor_id","actor_id",actor);
			
//			Table t_ijoin14 = film.i_join("film_id","film_id" , inventory); //film,rental,customer
//			Table t_ijoin15 = t_ijoin14.i_join("inventory_id", "inventory_id", rental);
//			Table t_ijoin16 = t_ijoin15.i_join("customer_id", "customer_id", customer);
			
			
	        
			end_time = System.nanoTime();
			
			duration = (double) (end_time - start_time) / 1e6;
			
			fw.write(operation);
			fw.write(String.valueOf(duration));
			fw.write("\n");
			
			
			System.out.println("\t\t" + duration + " millisecs");
			
		}
		fw.close();
    } // main

	public int getRandomNumber(int min, int max) {
	    return (int) ((Math.random() * (max - min)) + min);
	}
}
