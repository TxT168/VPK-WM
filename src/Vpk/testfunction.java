package Vpk;

import CompareVPK.OthersMethod;
import EmbedAndExtract.OtherEmbedAndExtract;
import GetData.GetImage2Array;

import java.math.BigInteger;
import java.util.*;

public class testfunction {
    public static void binString(){
//        String sk = "nihao";
//        String tempBinHashDecimalArrMSB = DataProcess2.testencryptMD5(sk.concat("decimalArrBinStrMSB"));
//        BigInteger temp = new BigInteger(tempBinHashDecimalArrMSB,2);
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        a.add("1");
        a.add("2");
        a.add("3");
        b.add("1");
        b.add("3");
        // a.removeAll(b);]
        System.out.println(a);
        Collections.reverse(a);
        System.out.println(a);
    }

    public static void main(String[] args) {
        // Test
        String[] testarrtributes = {"Elevation"};
        double[] testprob = {0.2};

        String imgPath = "/Users/ltc/PycharmProjects/BinGraph/imageResource/WWF_binary.png";
        int[][] rgbArray = GetImage2Array.getWatermark(imgPath);
        int width = rgbArray[0].length;    // 水印图片的宽度
        int height = rgbArray.length;      // 水印图片的高度
        int[][] EmbedWMPos = new int[height][width];
        System.out.println("图片的宽度："+ width + "    高度：" + height);
        String tablename = "forest";
        int tupleLimitStart = 0;
        int tupleLimit = 30000;
        int remainTupleNum = 3000;
        String sk = "100101000111";
        int lsbSize = 1;
        int msbSize = 16;
        double[] probs = {0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3};
        String[] arrtributes = {"Elevation", "Aspect", "Slope", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm", "Horizontal_Fire_Points"};
        String[] delArrs = {};

        // HashMap<String, Integer> origin_vpk = OthersMethod.genVPK(arrtributes, rgbArray, EmbedWMPos, tablename, tupleLimit, lsbSize, sk);     //在forest表中生成VPK
        // System.out.println("我的方法结果：");
        // HashMap<String, Integer> origin_vpk1 = DataProcess2.genVPK4(probs, tablename, tupleLimitStart, tupleLimit, sk, lsbSize, arrtributes, rgbArray, EmbedWMPos, msbSize);     //在forest表中生成VPK
        // DataProcess2.evaluateIndicator_One(origin_vpk1, tupleLimit, tupleLimitStart);

//        System.out.println("对比论文的结果：");
//        HashMap<String, Integer> origin_vpk2 = OthersMethod.genVPK(arrtributes, rgbArray, EmbedWMPos, tablename, tupleLimitStart, tupleLimit, lsbSize, sk, msbSize);     //在forest表中生成VPK
//        DataProcess2.evaluateIndicator_One(origin_vpk2, tupleLimit, tupleLimitStart);
//
//        int[][] ExtractWM = OtherEmbedAndExtract.ExtractWM(width, height, lsbSize, tablename,
//                arrtributes, tupleLimitStart, tupleLimit, sk, delArrs, remainTupleNum, msbSize);
//        GetImage2Array.writeImageFromArray("img/Test-Extract-WWF-Logo.png", "png", ExtractWM);

        binString();
    }
}
