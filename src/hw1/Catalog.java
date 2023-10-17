package hw1;

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
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {
	private class Inner{
		public String pkeyField;
		public HeapFile table;
		public String name;
		public Integer id;
		public Inner(Integer id, HeapFile table, String pkeyField,String name) {
	            this.id = id;
	            this.table = table;
	            this.pkeyField = pkeyField;
	            this.name = name;
	        }
	    public HeapFile getHeapFile() {
	    	System.out.println("getHeapFile");
	            return this.table;
	        }
	    public String getPkeyField() {
	            return this.pkeyField;
	        }
	    public String getName() {
	            return this.name;
	        }
	    public Integer getId() {
            return this.id;
        }
	}
	public HashMap<String,Inner> newCatalog;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    	this.newCatalog = new HashMap<String,Inner>();  
  
    	//your code here
    	
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified HeapFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name conflict exists, use the last table to be added as the table for a given name.
     * 是不加入的意思？？？？？？
     * @param pkeyField the name of the primary key field
     */
    public void addTable(HeapFile file, String name, String pkeyField) {
  
    	Inner newTable = new Inner(file.getId(),file,pkeyField,name);
    	newCatalog.put(name, newTable);
    	//your code here
    }
    
    //???????这啥意思？
    public void addTable(HeapFile file, String name) {
        addTable(file,name,"");
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name){
    	//your code here
    	return newCatalog.get(name).getId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	//your code here
    	for(Inner i:newCatalog.values()) {
    		System.out.println(i.getId());
    		if(tableid ==i.getId()) {
    			System.out.println(tableid+"inner");
    			HeapFile newhf = i.getHeapFile();
    			System.out.println("tupledesc");
    			return newhf.getTupleDesc();
    		}
    	}
    	throw new NoSuchElementException("not exist");
    }

    /**
     * Returns the HeapFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the HeapFile.getId()
     *     function passed to addTable
     */
    public HeapFile getDbFile(int tableid) throws NoSuchElementException {
    	//your code here
    	for(Inner i:newCatalog.values()) {
    		if(tableid ==i.getId()) {
    			return i.getHeapFile();
    		}
    	}
    	throw new NoSuchElementException("not exist");
    }

    /** Delete all tables from the catalog */
    public void clear() {
    	newCatalog.clear();
    	//your code here
    }

    public String getPrimaryKey(int tableid) {
    	//your code here
    	for(Inner i:newCatalog.values()) {
    		if(tableid ==i.getId()) {
    			return i.getPkeyField();
    		}
    	}
    	return null;
    }

    public Iterator<Integer> tableIdIterator() {
    	//your code here
    	ArrayList<Integer> keys = new ArrayList<>();
    	for(Inner i : newCatalog.values()) {
    		keys.add(i.getId());
    	}
    	
    	return keys.iterator();
    }

    public String getTableName(int id) {
    	//your code here
    	for(Inner i : newCatalog.values()) {
    		if(i.getId()==id) {
    			return i.getName();
    		}
    	}
    	throw new NoSuchElementException("not exist");
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
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
                        types.add(Type.INT);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING);
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
                HeapFile tabHf = new HeapFile(new File("testfiles/" + name + ".dat"), t);
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

