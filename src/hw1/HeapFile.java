package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;

	public File f;
	public TupleDesc type;
	public int heapFileId;
	public int pageNum;
	//public int pageid;
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param type type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		this.f=f;
		this.type=type;
		this.heapFileId = getId();
		pageNum = 1;
		//your code here
	}

	public File getFile() {
		//your code here
		return this.f;
	}

	public TupleDesc getTupleDesc() {
		System.out.println("td2");
		//your code here
		return this.type;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 *
	 */
	public HeapPage readPage(int id) {
		RandomAccessFile newFile = null;
		try {
			newFile = new RandomAccessFile(this.f,"rws");
		}
		catch (FileNotFoundException e){
			e.printStackTrace();
			System.out.println("Problem on creating new raf ");
		}
		byte[] data = new byte[PAGE_SIZE];///!!!!
		try {
			assert newFile!=null;
			newFile.seek(id*PAGE_SIZE);
			newFile.read(data);
		}
		catch(IOException e){
			e.printStackTrace();
			System.out.println("Problem on seek and read");
		}
		HeapPage newhp = null;
		try {
			newhp = new HeapPage(id,data, this.heapFileId);//!!!!
		}
		catch(IOException e) {
			e.printStackTrace();
			System.out.println("Problem on creating new hp");
		}
		try {
			newFile.close();
		}
		catch(IOException e) {
			e.printStackTrace();
			System.out.println("Problem on closing raf");
		}
		//your code here
		return newhp;
	}

	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return f.hashCode();//?
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(this.f, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Problem on creating raf ");
		}
		int pid = p.getId();
		try {
			assert raf!=null;
			raf.seek(pid*PAGE_SIZE);
			raf.write(p.getPageData());///???
		}/////?????????????????????????????????
		catch(IOException e){
			e.printStackTrace();
			System.out.println("Problem on seek and read");
		}
		try {
			raf.close();
		}
		catch(IOException e) {
			e.printStackTrace();
			System.out.println("Problem on closing raf");
		}
		//your code here
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception
	 */
	public HeapPage addTuple(Tuple t)  {

		for(int i =0;i<pageNum;i++) {
			try {
				HeapPage pageRead = readPage(i);
				pageRead.addTuple(t);
				writePage(pageRead);
				return pageRead;
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		byte[] data = new byte[PAGE_SIZE];///!!!!
		HeapPage newPage=null;
		try {
			newPage = new HeapPage(pageNum,data,this.heapFileId);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			assert newPage!=null;
			newPage.addTuple(t);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		writePage(newPage);
		pageNum++;
		return newPage;

	}
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception{
		HeapPage hp = readPage(t.getPid());
		hp.deleteTuple(t);
		writePage(hp);
	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		ArrayList<Tuple> tupleiterator = new ArrayList<>();
		for(int i=0;i<pageNum;i++) {
			HeapPage hp = readPage(i);
			Iterator<Tuple>iterator = hp.iterator();
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				tupleiterator.add(tuple);
			}
		}
		//your code here
		return tupleiterator;
	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {

		return pageNum;

		//your code here

	}
}
