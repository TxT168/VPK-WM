package Test;

import GetData.GetImage2Array;
import Vpk.DataProcess2;

import java.util.HashMap;

public class testmain {
    public static  void test(){

    }

    public static void main(String[] args) {
        // Test
        String[] testarrtribute = {"Elevation"};
        double[] testprob = {0.2};

        String imgPath = "/Users/ltc/PycharmProjects/BinGraph/imageResource/WWF_binary.png";
        int[][] rgbArray = GetImage2Array.getWatermark(imgPath);
        String tablename = "test";
        int tupleLimit = 100;
        String sk = "100101000111";
        int lsbSize = 1;
        double[] probs = {0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35};
        String[] arrtributes = {"Elevation", "Aspect", "Slope", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm", "Horizontal_Fire_Points"};
        String[] testarrtributes = {"a1","a2","a3","a4","a5","a6","a7","a8","a9","a10"};

        // HashMap<String, Integer> origin_vpk = DataProcess2.genVPK4(probs, tablename, tupleLimit, sk, lsbSize, testarrtributes, rgbArray);     //在forest表中生成VPK
        // List<String> delArr_vpk = genVPK4(del_prob,"test","a2","a3","a4","a5","a6","a7","a8","a9","a10");
        // DataProcess2.evaluateIndicator_One(origin_vpk, tupleLimit);

    }
}
