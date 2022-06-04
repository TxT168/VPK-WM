package Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RuntimeFunction {
    public static void main(String[] args) {
        Process proc;
        try {
            String testdata = "1,0,0;0,1,0;0,0,1";
            String[] args1 = new String[]{"/usr/local/Caskroom/miniconda/base/envs/bert-nlp/bin/python",
                    "/Users/ltc/PycharmProjects/BinGraph/printBinGraph.py", testdata};
            proc = Runtime.getRuntime().exec(args1);
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

