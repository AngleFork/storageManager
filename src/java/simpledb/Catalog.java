package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. 
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk. And currently there is 
 * only one global database holds all of tables.
 */

public class Catalog {

    private ConcurrentHashMap<Integer, TableInfo> _tables; 
    /**
     * Provides connections between table name to table identifier.
     */
    private ConcurrentHashMap<String, Integer> _table_ids;
    public static class TableInfo{
        private DbFile      _tb_file;
        private String      _tb_name;
        private TupleDesc _tb_schema;
        private String      _tb_pkeyfield;
        public TableInfo(DbFile file, String name, TupleDesc td, String pkeyField){
            this._tb_file    = file;
            this._tb_name = name;
            this._tb_schema = td;
            this._tb_pkeyfield =  pkeyField;
        }
        public DbFile getDbFile(){
            return this._tb_file;
        }
        public String getTableName(){
            return this._tb_name;
        }
        public TupleDesc getTupleDesc(){
            return this._tb_schema;
        }
        public String getPrimaryKey(){
            return this._tb_pkeyfield;
        }
    }
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        this._tables = new ConcurrentHashMap<Integer, TableInfo>();
        this._table_ids = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     *      conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        //Problems: How to detect name conflicts?
        //For now, we are not dealing with DbFile
        Integer new_tid = new Integer(file.getId());
        if(Integer prev_tid = this._table_ids.replace(name, new_tid)){
            this._tables.remove(prev_tid);
        }
        this._tables.put(new_tid, new TableInfo(name, file.getTupleDesc(),
                    pkeyField))
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(Integer tid = this._table_ids.get(name)){
            return tid.intValue();
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if(TableInfo tb_info = this._tables.get(new Integer(tableid))){
            return tb_info.getTupleDesc();
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if(TableInfo tb_info = this._tables.get(new Integer(tableid))){
            return tb_info.getDbFile();
        }
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if(TableInfo tb_info = this._tables.get(new Integer(tableid))){
            return tb_info.getPrimaryKey();
        }
        return null
    }
    //TODO:
    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return null;
    }

    public String getTableName(int id) {
        // some code goes here
        if(TableInfo tb_info = this._tables.get(new Integer(id))){
            return tb_info.getTableName();
        }
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        this._tables.clear();
        this._table_ids.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(catalogFile).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

