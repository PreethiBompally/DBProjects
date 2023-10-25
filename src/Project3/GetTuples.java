package Project3;

import java.util.ArrayList;
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
    	int[] rowCounter = { 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 25000, 50000 };

    	String file_path = Paths.get("").toAbsolutePath().toString()+"\\src\\Project3\\timestamp.txt";
		File timestamp = new File(file_path);
		FileWriter fw = new FileWriter(file_path);
    	
    	var test = new GetTuples();
    	
    	test.addRelSchema("actor","actor_id first_name last_name last_update","Integer String String String","actor_id",new String[][] {  });
    	test.addRelSchema("customer","customer_id store_id first_name last_name email address_id activebool create_date last_update active","Integer Integer String String String Integer Integer String String Integer","customer_id",new String[][] { {"String_id","String","String_id"},{"store_id","store","store_id"}});
    	test.addRelSchema("film","film_id title description release_year language_id original_language_id rental_duration rental_rate length replacement_cost rating last_update special_features fulltxt","Integer String String Integer Integer Integer Integer Float Integer Float String String String String","film_id",new String[][] { {"language_id","language","language_id"},{"original_language_id","language","language_id"}});
    	test.addRelSchema("film_actor","actor_id film_id last_update","Integer Integer String","actor_id film_id",new String[][] { {"actor_id","actor","actor_id"},{"film_id","film","film_id"}});
    	test.addRelSchema("rental","rental_id rental_date inventory_id customer_id return_date staff_id last_update","Integer String Integer Integer String Integer String","rental_id",new String[][] { {"customer_id","customer","customer_id"},{"inventory_id","inventory","inventory_id"},{"staff_id","staff","staff_id"}});
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
			
			GetData data = new GetData();
			List<Comparable[]> tups = data.getTuples("actor",rowCounter[i]);
			for(Comparable[] tup:tups){
				actor.insert(tup);
			}
			List<Comparable[]> tups1 = data.getTuples("customer",rowCounter[i]);
			for(Comparable[] tup1:tups1){
				customer.insert(tup1);
			}
			List<Comparable[]> tups2 = data.getTuples("film",rowCounter[i]);
			for(Comparable[] tup2:tups2){
				film.insert(tup2);
			}
			List<Comparable[]> tups3 = data.getTuples("film_actor",rowCounter[i]);
			for(Comparable[] tup3:tups3){
				film_actor.insert(tup3);
			}
			List<Comparable[]> tups4 = data.getTuples("rental",rowCounter[i]);
			for(Comparable[] tup4:tups4){
				rental.insert(tup4);
			}
			Random ran = new Random();
			start_time = System.nanoTime();
			
//			 non-indexed select
//			out.println ();
//			operation = "non-indexed-select for "+rowCounter[i]+" rows for ";
//	        var t_niselect = actor.nonIndexSelect(new KeyType(136));
//	        t_niselect.print ();
			
			// indexed select
//			out.println ();
//			operation = "indexed-select for "+rowCounter[i]+" rows : ";
//	        var t_iselect = actor.select(new KeyType(136));
//	        t_iselect.print ();

//			non-indexed join
			out.println ();
			operation = "non-indexed-join for "+rowCounter[i]+" rows : ";
			var t_nijoin = actor.join("film_id","film_id" , film_actor);
	        t_nijoin.print();
			
			//indexed join
//			non-indexed join
//			out.println ();
//			operation = "non-indexed-join for "+rowCounter[i]+" rows : ";
//			var t_ijoin = actor.i_join("film_id","film_id" , film_actor);
//	        t_ijoin.print();
			
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
