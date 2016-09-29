package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     *
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // anoymous class can not has a constructor.
        return new Iterator<TDItem>(){
            int current = 0;
            public boolean hasNext(){
                return this.current < descEntries.length;
            }
            public TDItem next(){
                return descEntries[this.current++];
            }
            public void remove(){
                throw new UnsupportedOperationException();
            }
        };
    }
    

    private static final long serialVersionUID = 1L;
    /**
     * The entries of the tuple description
     */
    private TDItem[] descEntries;
    /**
     * The size of the tuple along with the tuple description in bytes
     */
    private int size;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        Arrays.toString(fieldAr);
        this.descEntries = new TDItem[typeAr.length];
        this.size = 0;
        for(int i = 0; i < typeAr.length; i++){
            this.descEntries[i] = new TDItem(typeAr[i], fieldAr[i]);
            this.size += typeAr[i].getLen();
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.descEntries = new TDItem[typeAr.length];
        this.size = 0;
        for(int i = 0; i < typeAr.length; i++){
            this.descEntries[i] = new TDItem(typeAr[i], "");
            this.size += typeAr[i].getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.descEntries.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= 0 && i < descEntries.length){
            return descEntries[i].fieldName;
        }else{
            throw  new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= 0 && i < descEntries.length){
            return descEntries[i].fieldType;
        }else{
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i = 0; i < descEntries.length; i++){
            if(descEntries[i].fieldName.equals(name)){
                return i; 
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int length = td1.numFields() + td2.numFields();
        Type[] types = new Type[length];
        String[] fdnames = new String[length];
        int current = 0;
        for(TDItem item: td1.descEntries){
            types[current] = item.fieldType;
            fdnames[current++] = item.fieldName;
        }
        for(TDItem item: td2.descEntries){
            types[current] = item.fieldType;
            fdnames[current++] = item.fieldName;
        }
        return new TupleDesc(types, fdnames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(o instanceof TupleDesc){
            TupleDesc td = (TupleDesc) o;
            if(td.numFields() == this.numFields()){
                for(int i = 0; i < this.descEntries.length && 
                        i < ((TupleDesc) o).descEntries.length; i++){
                    if(this.descEntries[i].fieldType != 
                            ((TupleDesc) o).descEntries[i].fieldType){
                        return false;
                    }
                }
            }else{
                return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer str = new StringBuffer();
        int current = 0;
        for(TDItem item: this.descEntries){
            str.append(item.fieldType.toString()+"[" + current + "](");
            str.append(item.fieldName + "[" + current + "]),");
            current++;
        }
        return str.deleteCharAt(str.length()-1).toString();
    }
}
