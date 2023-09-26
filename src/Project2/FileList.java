package Project2;
/*******************************************************************************
 * @file  FileList.java
 *
 * @author   John Miller
 */

import java.io.*;
import static java.lang.System.out;
import java.util.*;


/*******************************************************************************
 * This class allows data tuples/tuples (e.g., those making up a relational table) to be stored in a random access file.  
 * This implementation requires that each tuple be packed into a fixed length byte array.
 */
public class FileList extends AbstractList <Comparable []> implements List <Comparable []>, RandomAccess,Serializable
{
    /** File extension for data files.
     */
    private static final String EXT = ".dat";

    /** The random access file that holds the tuples.
     */
    transient private RandomAccessFile file;

    /** The name of table.
     */
    private final String tableName;

    /** The number bytes required to store a "packed tuple"/record.
     */
    private final int recordSize;

    /** Counter for the number of tuples in this list.
     */
    private int nRecords = 0;
    
   
	/** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;
    
    private int maxRecordCount = 500;
    /**
     * attributeSize[0] : tuple0
     * attributeSize[0][0] : tuple0 attribate0 Len
     */
    private int[][] attributeSize ;   
    
    
    //len the same as RandomAccessFile
    public static final int tupleLongLen = 8;
    public static final int tupleIntLen = 4;
    public static final int tupleShortLen = 2;
    public static final int tupleBytetLen = 1;
    
    public static final int tupleFloatLen = 4;
    public static final int tupleDoubleLen = 8;
    
    public static final int tupleCharacterLen = 2;
    public static final int tupleStringLen = 50;   //max Len
    
    
    /***************************************************************************
     * Construct a FileList.
     * @param _tableName   the name of the table
     * @param _recordSize  the size of tuple in bytes.
     */
    public FileList (String _tableName , int _recordSize, Class [] _domain)
    {
        tableName  = _tableName;
        recordSize = _recordSize;
        domain = _domain;
        attributeSize = new int[maxRecordCount][_domain.length];
        try {
            file = new RandomAccessFile (tableName + EXT, "rw");
        } catch (FileNotFoundException ex) {
            file = null;
            out.println ("FileList.constructor: unable to open - " + ex);
        } // try
    } // constructor
    
    
    /***************************************************************************
     * Add a new tuple into the file list by packing it into a record 
     * and writing this record to the random access file.  
     * Write the record either at the end-of-file 
     * or into a empty slot.
     * 
     * @param tuple  the tuple to add
     * @return  whether the addition succeeded
     */
    public boolean add (Comparable [] tuple)
    {
    	
        byte [] record = pack(tuple); 
        
        
        if (record.length > recordSize) {
            out.println ("FileList.add: exceeds maximum length " + record.length);
            return false;
        } // if

        try {
			file.seek(file.length());
			file.write(record);
		} catch (IOException e) {
			e.printStackTrace();
		}
          
        this.nRecords ++;
        return true;
    } // add
    
    /*
    * @Description: pack a Comparable tuple to byte[] tuple
    * @param  Comparable [] tuple  
    * @return byte[] tuple  
    * @throws
     */
    public byte[] pack(Comparable [] tuple){
    	byte[] result = null;
    	
    	ByteArrayOutputStream bop = new ByteArrayOutputStream();
    	DataOutputStream dop = new DataOutputStream(bop);
    	
    	for(int i = 0; i<tuple.length;i++){
    		bop.reset();
    		byte[] buf = null;
    		try {
    			
	    		if(tuple[i] instanceof Long ){
	    			dop.writeLong((long) tuple[i]);
	        	}else if(tuple[i] instanceof Integer ){
	        		
	        		dop.writeInt((int) tuple[i]);
	        	}else if(tuple[i] instanceof Short ){
	        		dop.writeShort((Short)tuple[i]);
	        	}else if(tuple[i] instanceof Byte ){
	        		dop.writeByte((Byte)tuple[i]);
	        	}else if(tuple[i] instanceof Double  || tuple[i] instanceof Float ){
	        		dop.writeDouble((Double)tuple[i]);
	        	}else if(tuple[i] instanceof Character){
	        		dop.writeChar((Character)tuple[i]);
	        	}else if(tuple[i] instanceof String){			
	        		dop.writeUTF((String) tuple[i]);
	        	}
	    		buf = bop.toByteArray();
        		attributeSize[nRecords][i] = buf.length;
    		} catch (IOException e) {
				e.printStackTrace();
			}
    		result = byteMerger(result,buf);
    		
        	
    	}
    	return result;
    }
    
    public  byte[] byteMerger(byte[] byte_1, byte[] byte_2){
    	
    	if(byte_1 == null && byte_2 ==null){
    		return null;
    	}else if(byte_1 == null){
    		return byte_2;
    	}else if(byte_2 == null){
    		return byte_1;
    	}
    	
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];  
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);  
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);  
        return byte_3;  
    }  

    public  Comparable [] unpack(byte[] result,int index){
    	
    	Comparable [] tup  = new Comparable[domain.length];
    	byte[] buf  = null;
    	ByteArrayInputStream bip  = null;
    	DataInputStream dip ;
    	int resultIndex = 0;
    	for(int i = 0;i<domain.length;i++){
    		
    		try{
        		buf = new byte[attributeSize[index][i]];
        		System.arraycopy(result, resultIndex , buf, 0, buf.length);
        		resultIndex = resultIndex +  buf.length;
        		bip = new ByteArrayInputStream(buf);
        		dip = new DataInputStream(bip);

        		if(domain[i].equals(Long.class) ){
        			long temp = dip.readLong();
        			tup[i] = temp;
            	}else if(domain[i].equals(Integer.class) ){
            		int temp = dip.readInt();
            		tup[i] = temp;
            	}else if(domain[i].equals(Short.class) ){
            		short temp = dip.readShort();
            		tup[i] = temp;
            	}else if(domain[i].equals(Byte.class) ){
            		byte temp = dip.readByte();
            		tup[i] = temp;
            	}else if(domain[i].equals(Float.class)){
            		double temp = dip.readDouble();
            		tup[i] = temp;
            	}else if(domain[i].equals(Double.class)){
            		double temp = dip.readDouble();
            		tup[i] = temp;
            	}else if(domain[i].equals(Character.class)){
            		char temp = dip.readChar();
            		tup[i] = temp;
            	}else if(domain[i].equals(String.class)){
            		String temp = dip.readUTF();
            		tup[i] = temp;
            	}
        			
    		}catch(IOException e) {
				e.printStackTrace();
			}	
    	}
    	return tup;
    }

    /***************************************************************************
     * Get the ith tuple by seeking to the correct file position and reading the
     * record.
     * @param i  the index of the tuple to get
     * @return  the ith tuple
     */
    public Comparable [] get (int index)
    {
    	if(index + 1 > nRecords){
    		System.out.println("Out of Index");
    		return null;
    	}
    	int thisRecordSize  = 0 ;
    	for(int i=0;i<domain.length;i++){
    		thisRecordSize = thisRecordSize + attributeSize[index][i];
    	}
    	int startIndex = 0;
    	for(int i = 0; i<index;i++){
    		for(int j=0;j<domain.length;j++){
    			startIndex = startIndex + attributeSize[i][j];
    		}
    	}
        byte [] record = new byte[thisRecordSize];
        try {
        	this.file.seek(startIndex);
			this.file.read(record, 0, thisRecordSize);
		} catch (IOException e) {
			e.printStackTrace();
		}
                
        
        Comparable [] tuple = unpack(record,index); 

        return tuple;
    } // get

    public RandomAccessFile getFile() {
		return file;
	}


	/***************************************************************************
     * Return the size of the file list in terms of the number of tuples/records.
     * @return  the number of tuples
     */
    public int size ()
    {
        return nRecords;
    } // size

    /***************************************************************************
     * Close the file.
     */
    public void close ()
    {
        try {
            file.close ();
        } catch (IOException ex) {
            out.println ("FileList.close: unable to close - " + ex);
        } // try
    } // close

} // FileList class
