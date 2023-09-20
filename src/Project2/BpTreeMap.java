package Project2;


/************************************************************************************
 * @file BpTreeMap.java
 *
 * @author  John Miller
 */

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

import static java.lang.Math.ceil;
import static java.lang.System.out;

/************************************************************************************
 * The BpTreeMap class provides B+Tree maps.  B+Trees are used as multi-level index
 * structures that provide efficient access for both point queries and range queries.
 * All keys will be at the leaf level with leaf nodes linked by references.
 * Internal nodes will contain divider keys such that each divider key corresponds to
 * the largest key in its left subtree (largest left).  Keys in left subtree are "<=",
 * while keys in right subtree are ">".
 */
public class BpTreeMap <K extends Comparable <K>, V>
       extends AbstractMap <K, V>
       implements Serializable, Cloneable, SortedMap <K, V>
{
    /** The debug flag
     */
    private static final boolean DEBUG = true;

    /** The maximum fanout (number of children) for a B+Tree node.
     *  May wish to increase for better performance for Program 3.
     */
    private static final int ORDER = 5;

    /** The maximum number of keys
     */
    private static final int MAX = ORDER - 1;

    /** The half the number of keys (larger half)
     */
    private static final int MID = ORDER / 2;

    /** The class for type K.
     */
    private final Class <K> classK;

    /** The class for type V.
     */
    private final Class <V> classV;

    /********************************************************************************
     * This inner class defines nodes that are stored in the B+tree map.
     */
    private class Node
    {
        boolean   isLeaf;                                             // whether the node is a leaf 
        int       nKeys;                                              // number of active keys
        K []      key;                                                // array of keys
        Object [] ref;                                                // array of references/pointers

        /****************************************************************************
         * Construct a BpTree node.
         * @param isLeaf  whether the node is a leaf
         */
        @SuppressWarnings("unchecked")
        Node (boolean _isLeaf)
        {
            isLeaf = _isLeaf;
            nKeys  = 0;
            key    = (K []) Array.newInstance (classK, MAX);
            if (isLeaf) ref = new Object [ORDER];
            else        ref = (Node []) Array.newInstance (Node.class, ORDER);
        } // constructor

        /****************************************************************************
         * Copy keys and ref from node n to this node.
         * @param n     the node to copy from
         * @param from  where in n to start copying from
         * @param num   the number of keys/refs to copy
         */
        void copy (Node n, int from, int num)
        {
            nKeys = num;
            for (var i = 0; i < num; i++) { key[i] = n.key[from+i]; ref[i] = n.ref[from+i]; }
            ref[num] = n.ref[from+num];
        } // copy

        /****************************************************************************
         * Find the "<=" match position in this node.
         * @param k  the key to be matched.
         * @return  the position of match within node, where nKeys indicates no match
         */
        int find (K k)
        {
            for (var i  = 0; i < nKeys; i++) if (k.compareTo (key[i]) <= 0) return i;
            return nKeys;
        } // find
        
        /****************************************************************************
         * Overriding toString method to print the Node. Prints out the keys.
         */
        @Override
        public String toString () 
        {
            return Arrays.deepToString (key);
        } // toString

    } // Node inner class

    /** The root of the B+Tree
     */
    private Node root;

    /** The first (leftmost) leaf in the B+Tree
     */
    private final Node firstLeaf;

    /** Flag indicating whether a split at the level below has occurred that needs to be handled
     */
    private boolean hasSplit = false;

    /** The counter for the number nodes accessed (for performance testing)
     */
    private int count = 0;

    /** The counter for the total number of keys in the B+Tree Map
     */
    private int keyCount = 0;

    /********************************************************************************
     * Construct an empty B+Tree map.
     * @param _classK  the class for keys (K)
     * @param _classV  the class for values (V)
     */
    public BpTreeMap (Class <K> _classK, Class <V> _classV)
    {
        classK    = _classK;
        classV    = _classV;
        root      = new Node (true);
        firstLeaf = root;
    } // constructor

    /********************************************************************************
     * Return null to use the natural order based on the key type.  This requires the
     * key type to implement Comparable.
     */
    public Comparator <? super K> comparator () 
    {
        return null;
    } // comparator

    /********************************************************************************
     * Return a set containing all the entries as pairs of keys and values.
     * @return  the set view of the map
     */
    public Set <Map.Entry <K, V>> entrySet ()
    {
        var enSet = new HashSet <Map.Entry <K, V>> ();

        //  T O   B E   I M P L E M E N T E D
        
        return enSet;
    } // entrySet

    /********************************************************************************
     * Given the key, look up the value in the B+Tree map.
     * @param key  the key used for look up
     * @return  the value associated with the key or null if not found
     */
    @SuppressWarnings("unchecked")
    public V get (Object key)
    {
        return find ((K) key, root);
    } // get

    /********************************************************************************
     * Put the key-value pair in the B+Tree map.
     * @param key    the key to insert
     * @param value  the value to insert
     * @return  null, not the previous value for this key
     */
    public V put (K key, V value)
    {
        keyCount++;
        insert (key, value, root);
        return null;
    } // put

    /********************************************************************************
     * Return the first (smallest) key in the B+Tree map.
     * @return  the first key in the B+Tree map.
     */
    public K firstKey () 
    {
        return firstLeaf.key[0];
    } // firstKey

    /********************************************************************************
     * Return the last (largest) key in the B+Tree map.
     * @return  the last key in the B+Tree map.
     */
    public K lastKey () 
    {
        //  T O   B E   I M P L E M E N T E D

        return null;
    } // lastKey

    /********************************************************************************
     * Return the portion of the B+Tree map where key < toKey.
     * @return  the submap with keys in the range [firstKey, toKey)
     */
    public SortedMap <K,V> headMap (K toKey)
    {
        //  T O   B E   I M P L E M E N T E D

        return null;
    } // headMap

    /********************************************************************************
     * Return the portion of the B+Tree map where fromKey <= key.
     * @return  the submap with keys in the range [fromKey, lastKey]
     */
    public SortedMap <K,V> tailMap (K fromKey)
    {
        //  T O   B E   I M P L E M E N T E D

        return null;
    } // tailMap

    /********************************************************************************
     * Return the portion of the B+Tree map whose keys are between fromKey and toKey,
     * i.e., fromKey <= key < toKey.
     * @return  the submap with keys in the range [fromKey, toKey)
     */
    public SortedMap <K,V> subMap (K fromKey, K toKey)
    {
        //  T O   B E   I M P L E M E N T E D

        return null;
    } // subMap

    /********************************************************************************
     * Return the size (number of keys) in the B+Tree.
     * @return  the size of the B+Tree
     */
    public int size ()
    {
        return keyCount;
    } // size

    /********************************************************************************
     * Print the B+Tree using a pre-order traversal and indenting each level.
     * @param n      the current node to print
     * @param level  the current level of the B+Tree
     */
    @SuppressWarnings("unchecked")
    private void print (Node n, int level)
    {
        if (n == null) out.println ("print: unexpected null node");

        if (n == root) out.println ("BpTreeMap");
        out.println ("-------------------------------------------");

        for (var j = 0; j < level; j++) out.print ("\t");
        out.print ("[ . ");
        for (var i = 0; i < n.nKeys; i++) out.print (n.key[i] + " . ");
        out.println ("]");
        if ( ! n.isLeaf) {
            for (var i = 0; i <= n.nKeys; i++) print ((Node) n.ref[i], level + 1);
        } // if

        if (n == root) out.println ("-------------------------------------------");
    } // print

    /********************************************************************************
     * Recursive helper function for finding a key in a B+tree.
     * @param key  the key to find
     * @param n    the current node
     */
    @SuppressWarnings("unchecked")
    private V find (K key, Node n)
    {
        count++;
        var i = n.find (key);
        if (i < n.nKeys) {
            K k_i = n.key[i];
            if (n.isLeaf) return (key.compareTo (k_i) == 0) ? (V) n.ref[i] : null;
            else          return find (key, (Node) n.ref[i]);
        } else {
            return (n.isLeaf) ? null : find (key, (Node) n.ref[n.nKeys]);
        } // if
    } // find

    /********************************************************************************
     * Recursive helper function for inserting a key into a B+tree.
     * Wedge key-ref pair into node n, unless it is full in which case node n is split first
     * by allocating a right sibling node r and placing the lesser half of n's key in r.
     * After the split, the key-ref pair will be wedged into node n or r depending on
     * the value of the key.
     * A split also will require a key-ref pair to be inserted at the next level up.
     * @param key  the key to insert
     * @param ref  the value/node to insert
     * @param n    the current node
     * @return  the newly allocated right sibling node of n 
     */
    @SuppressWarnings("unchecked")
    private Node insert (K key, V ref, Node n)
    {
        out.println ("=============================================================");
        out.println ("insert: key = " + key);
        out.println ("=============================================================");

        Node r = null;                                                       // holder for right sibling node
//---------------------------------------------------------------------------   
        if (n.isLeaf) {                                                      // handle leaf node level

            if (n.nKeys < MAX) {                                             // current node is not full
                wedge (key, ref, n, n.find (key), true);                     // wedge (key, ref) pair at find position
            } else {
                r = split (n);                                               // split current node, return right sibling
                n.ref[n.nKeys] = r;                                          // link leaf n to leaf r
                if (n == root) root = makeRoot (n, n.key[n.nKeys-1], r);     // at root => make a new root
                if (key.compareTo (n.key [MID]) < 0)
                    wedge (key, ref, n, n.find (key), true);                 // wedge (key, ref) pair into node n
                else
                    wedge (key, ref, r, r.find (key), true);                 // wedge (key, ref) pair into node r
            } // if
//---------------------------------------------------------------------------   
        } else {                                                             // handle internal node level

            var i = n.find (key);                                            // find "<=" position
            r = insert (key, ref, (Node) n.ref[i]);                          // recursive call to insert
            if (DEBUG) out.println ("insert: handle internal node level");

                //  T O   B E   I M P L E M E N T E D

        } // if
//---------------------------------------------------------------------------   
        if (DEBUG) print (root, 0);
        return r;                                                            // return right sibling node
    } // insert

    /********************************************************************************
     * Make a new root, linking to left and right child node, separated by a divider key.
     * @param ref0  the reference to the left child node
     * @param key0  the divider key - largest left
     * @param ref1  the reference to the right child node
     * @return  the node for the new root
     */
    private Node makeRoot (Node ref0, K key0, Node ref1)
    {
        var nr    = new Node (false);                                 // make a node to become the new root
        nr.nKeys  = 1;                                                
        nr.ref[0] = ref0;                                             // reference to left node
        nr.key[0] = key0;                                             // divider key - largest left
        nr.ref[1] = ref1;                                             // reference to right node
        return nr;
    } // makeRoot
    
    /********************************************************************************
     * Wedge the key-ref pair into node n.  Shift right to make room if needed.
     * @param key   the key to insert
     * @param ref   the value/node to insert
     * @param n     the current node
     * @param i     the insertion position within node n
     * @param left  whether to start from the left side of the key
     * @return  whether wedge succeeded (i.e., no duplicate)
     */
    private boolean wedge (K key, Object ref, Node n, int i, boolean left)
    {
        if (i < n.nKeys && key.compareTo (n.key[i]) == 0) {
            out.println ("BpTreeMap.insert: attempt to insert duplicate key = " + key);
            return false;
        } // if

        n.ref[n.nKeys + 1] = n.ref[n.nKeys];                          // preserving the last ref
        for (var j = n.nKeys; j > i; j--) {
            n.key[j] = n.key[j-1];                                    // make room: shift keys right
            if (left || j > i + 1) n.ref[j] = n.ref[j-1];             // make room: shift refs right
        } // for
        n.key[i] = key;                                               // place new key
        if (left) n.ref[i] = ref; else n.ref[i+1] = ref;              // place new ref
        n.nKeys++;                                                    // increment number of keys
        return true;
    } // wedge

    /********************************************************************************
     * Split leaf node n and return the newly created right sibling node r.  The
     * bigger half stays in current node n, with the remaining lesser half going into r.
     * @param n  the current leaf node
     * @return  the new right sibling leaf node r
     */
    private Node split (Node n)
    {
        var r = new Node (true);                                      // make a right sibling node (r)
        r.copy (n, MID, MAX-MID);                                     // copy second half to node r    
        n.nKeys = MID;                                                // reset the number of keys in node n
        return r;                                                     // return right sibling
    } // splt

    /********************************************************************************
     * Split internal node n and return the newly created right sibling node r.  The
     * bigger half stays in current node n, with the remaining lesser half going into r.
     * @param n  the current internal node
     * @return  the new right sibling internal node r
     */
    private Node splitI (Node n)
    {
        var r = new Node (false);                                     // make a right sibling node (r)

                //  T O   B E   I M P L E M E N T E D

        return r;
    } // spltI

    /********************************************************************************
     * The main method used for testing.  Also test for more keys and with RANDOMLY true.
     * @param  the command-line arguments (args[0] gives number of keys to insert)
     */
    public static void main (String [] args)
    {
        var totalKeys = 30;                    
        var RANDOMLY  = false;
        var bpt       = new BpTreeMap <Integer, Integer> (Integer.class, Integer.class);
        if (args.length == 1) totalKeys = Integer.valueOf (args[0]);
   
        if (RANDOMLY) {
            Random rng = new Random ();
            for (var i = 1; i <= totalKeys; i += 2) bpt.put (rng.nextInt (2 * totalKeys), i * i);
        } else {
            for (var i = 1; i <= totalKeys; i += 2) bpt.put (i, i * i);
        } // if

        bpt.print (bpt.root, 0);
        for (var i = 0; i <= totalKeys; i++) {
            out.println ("key = " + i + " value = " + bpt.get (i));
        } // for
        out.println ("-------------------------------------------");
        out.println ("number of keys in BpTree = " + bpt.keyCount);
        out.println ("-------------------------------------------");
        out.println ("Average number of nodes accessed = " + bpt.count / (double) totalKeys);
    } // main

} // BpTreeMap class

