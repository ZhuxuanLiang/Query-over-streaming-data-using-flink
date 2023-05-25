package preprocess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class MergeAndSplitLargeFiles {
    public static void main(String[] args) throws IOException {
        String absolutePath = "/Users/phoebe/Documents/BDT/ip/myProject/";
        String filename1 = "testdata/customer.tbl";
        String filename2 = "testdata/lineitem.tbl";
        String filename3 = "testdata/nation.tbl";
        String filename4 = "testdata/orders.tbl";
        String filename5 = "testdata/region.tbl";
        String filename6 = "testdata/part.tbl";
        String filename7 = "testdata/supplier.tbl";
        Scanner sc1 = new Scanner(new File(absolutePath + filename1));
        Scanner sc2 = new Scanner(new File(absolutePath + filename2));
        Scanner sc3 = new Scanner(new File(absolutePath + filename3));
        Scanner sc4 = new Scanner(new File(absolutePath + filename4));
        Scanner sc5 = new Scanner(new File(absolutePath + filename5));
        Scanner sc6 = new Scanner(new File(absolutePath + filename6));
        Scanner sc7 = new Scanner(new File(absolutePath + filename7));
        BufferedWriter[] writers = new BufferedWriter[5];
        for(int i =0;i<5;i++){
            FileWriter fw = new FileWriter(absolutePath+"mergefile/MergeFile_large_"+i+".tbl");
            writers[i] =  new BufferedWriter(fw);
        }
        long count = 0;
        Random rand = new Random();
        while (sc1.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc1.nextLine();
            writers[r].write("0|insert|customer|" + tmp );
            writers[r].newLine();
        }

        while (sc2.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc2.nextLine();
            writers[r].write("0|insert|lineitem|" + tmp );
            writers[r].newLine();
        }

        while (sc3.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc3.nextLine();
            writers[r].write("0|insert|nation|" + tmp );
            writers[r].newLine();
        }

        while (sc4.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc4.nextLine();
            writers[r].write("0|insert|orders|" + tmp );
            writers[r].newLine();
        }

        while (sc5.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc5.nextLine();
            writers[r].write("0|insert|region|" + tmp );
            writers[r].newLine();
        }

        while (sc6.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc6.nextLine();
            writers[r].write("0|insert|part|" + tmp );
            writers[r].newLine();
        }
        while (sc7.hasNextLine()) {
            count++;
            int r = rand.nextInt(5);
            String tmp = sc7.nextLine();
            writers[r].write("0|insert|supplier|" + tmp );
            writers[r].newLine();
        }
        for(int i=0;i<5;i++){
            writers[i].close();
        }
        System.out.println(count);
    }
}
