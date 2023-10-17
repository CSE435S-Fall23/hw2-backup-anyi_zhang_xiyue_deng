package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapPage {

	private int id;//page id
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId; //table id



	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;//pageid
		this.tableId = tableId;//tableid

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();

		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];

		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];

			for (int i=0; i<tuples.length; i++) {
				tuples[i] = readNextTuple(dis, i);

			}
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();
	}

	public int getId() {
		//your code here
		return id;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		//your code here (int)Math.floor
		//becasue can not read arraylength!!!!!!!!!!!!!1

		return  (HeapFile.PAGE_SIZE-getHeaderSize())/td.getSize();//?size of
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {
		//your code here
		return (int)Math.ceil((8*HeapFile.PAGE_SIZE)/(8*td.getSize()+1)/8.0);
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test//第几个bit
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		/////////////
		if (s < 0 || s >= header.length * 8) {
			return false; // 超出范围，不占用
		}
		////////////////
		int numByte = s/8;
		int numBit = s-numByte*8;
		byte mask = (byte) (1 << numBit); // 创建一个掩码，该掩码在要查询的位上有一个1，其他位为0
		boolean isOccupied = (header[numByte] & mask) != 0;         //////!=
		//your code here
		if(isOccupied) {
			return true;
		}
		return false;
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		//your code here
		int numByte = s/8;
		int numBit = s-numByte*8;
		byte headerByte = header[numByte];
		byte bitmask = (byte) (1 << numBit);
		if (value) {
			// Set the bit to 1 to indicate the slot is occupied???
			headerByte |= bitmask;
		} else {
			// Clear the bit to 0 to indicate the slot is not occupied???
			headerByte &= ~bitmask;
		}
		header[numByte] = headerByte;

	}

	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		if(t.getDesc()!=td) {
			throw new Exception("nnonono");
		}
		for(int i =0;i<numSlots;i++) {
			if(slotOccupied(i)!=true) {
				tuples[i]=t;   //////??????
				tuples[i].setPid(id);
				tuples[i].setId(i);
				setSlotOccupied(i,true);
				return;
			}
		}
		throw new NoSuchElementException("unable to add valid tuple");
		//your code here
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		if(id!=t.getPid()) {
			throw new Exception("nnonono");
		}
		int i =t.getId();
		if(slotOccupied(i)) {

			setSlotOccupied(i,false);
			return;

		}
		throw new NoSuchElementException("not existed");
		//your code here
	}

	/**
	 * Suck up tuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, new IntField(field));


				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, new StringField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}

	/**
	 * Generates a byte array representing the contents of this page.
	 * Used to serialize this page to disk.
	 *
	 * The invariant here is that it should be possible to pass the byte
	 * array generated by getPageData to the HeapPage constructor and
	 * have it produce an identical HeapPage object.
	 *
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					dos.write(f.toByteArray());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page.
	 * @return
	 */
	public Iterator<Tuple> iterator() {

		ArrayList<Tuple> newiterator = new ArrayList<>();

		for(int i=0;i<numSlots;i++) {
			if(slotOccupied(i)) {
				newiterator.add(tuples[i]);
			}
		}

		return newiterator.iterator();
	}
}
