package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;



public class myTest {

    private Catalog c;

    @Before
    public void setup() {
        try {
            Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.out.println("unable to copy files");
            e.printStackTrace();
        }

        c = Database.getCatalog();
        c.loadSchema("testfiles/test.txt");

        c = Database.getCatalog();
        c.loadSchema("testfiles/A.txt");
    }

    @Test
    public void test1() {
        Query q = new Query("SELECT c1 FROM test");
        Relation r = q.execute();
        System.out.println(r.getTuples().toString());
        assertTrue(r.getTuples().size() == 1);

    }

    @Test
    public void test2() {
        Query q = new Query("SELECT MAX(a1) FROM A ");
        Relation r = q.execute();
        IntField agg = (IntField) (r.getTuples().get(0).getField(0));
        //System.out.println();
        assertTrue(agg.getValue() == 530);
        assertTrue(r.getTuples().size() == 1);

    }

}
