package preprocess;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

public class MergeFilefordelete {
    public static void main(String[] args) throws IOException {
        ArrayList<String> content = new ArrayList<>();
        String absolutePath = "/Users/phoebe/Documents/BDT/ip/myProject/";
        String filename1 = "mergefile/MergeFile_insert.tbl";
        Scanner sc1 = new Scanner(new File(absolutePath + filename1));
        while (sc1.hasNextLine()) {
            String tmp = sc1.nextLine();
            String[] s = tmp.split("\\|",3);
            content.add("0|delete|" + s[2]);
        }
        FileWriter fw = new FileWriter(absolutePath+"mergefile/MergeFile_delete.tbl");

        BufferedWriter writer = new BufferedWriter(fw);
        for (String i : content) {

                writer.write(i);
                writer.newLine();
            }
        writer.close();

    }
}


