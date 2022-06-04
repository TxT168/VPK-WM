package Vpk;

import CompareVPK.OthersMethod;
import GetData.GetImage2Array;
import EmbedAndExtract.OtherEmbedAndExtract;
import EmbedAndExtract.EmbedAndExtract;

import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

public class main {
    public static void test(){
        StringBuffer sb = new StringBuffer("She");
        sb.deleteCharAt(3);
        System.out.println(sb); //输出：Se
    }

    public static void otherMethodExperiment(){
        String imgName = "UTM";
        String imgPath = "/Users/ltc/PycharmProjects/BinGraph/imageResource/" + imgName + "_binary.png";
        String wmImgPath = "img/OtherMethod/"+imgName+"/Extract-" + imgName + "-Logo-step1-3-100000.png";
        String wmAndNoWmImgPath = "img/OtherMethod/"+imgName+"/wmAndNoWm/"+"/WmAndNoWmImg-" + imgName +".png";
        String delTupleRemainWM = "img/OtherMethod/"+imgName+"/delTuple/"+"/delTupleRemainWM-0%" + imgName + ".png";
        String delArrRemainWM = "img/OtherMethod/"+imgName+"/delArr/"+"/delArrRemainWM-"+ 1 + "-Arr-" + imgName + ".png";

        int[][] rgbArray = GetImage2Array.getWatermark(imgPath);
        int width = rgbArray[0].length;    // 水印图片的宽度
        int height = rgbArray.length;      // 水印图片的高度
        int[][] ExtractWM;
        int[][] EmbedWMPos = new int[height][width];
        int[][] WmAndNoWmPos = new int[height][width];
        String tablename = "forest";
        int tupleLimitStart = 1;
        int tupleLimit = 30000;
        int remainTupleNum = tupleLimit;
        String sk = "100101000111";
        int lsbSize = 1;
        int msbSize = 16;
        // float[] delProbs = {0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f};
        double[] probs = {0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3};
        double[] delProbs = {};
        String[] arrtributes = {"Elevation", "Aspect", "Slope", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm", "Horizontal_Fire_Points"};
        String[] testarrtributes = {"a1","a2","a3","a4","a5","a6","a7","a8","a9","a10"};
        String[] delArrs = {"Aspect", "Slope", "Horizontal_Hydrology"};

//        System.out.println("对比论文的结果：");
//        HashMap<String, Integer> origin_vpk2 = OthersMethod.genVPK(arrtributes, rgbArray, EmbedWMPos, tablename, tupleLimitStart, tupleLimit, lsbSize, sk, msbSize);     //在forest表中生成VPK
//        DataProcess2.evaluateIndicator_One(origin_vpk2, tupleLimit, tupleLimitStart);

        // 提取图片水印
         ExtractWM = OtherEmbedAndExtract.ExtractWM(width, height, lsbSize, tablename,
                 arrtributes, tupleLimitStart, tupleLimit, sk, delArrs, remainTupleNum, msbSize);
         GetImage2Array.writeImageFromArray(wmImgPath, "png", ExtractWM);

         float cf = DataProcess2.evaluateIndicator_Two(rgbArray, ExtractWM, height, width);
         System.out.println("图片水印提取正确的像素比：" + cf);
    }

    public static void main(String[] args) {
        // Test
        String[] testarrtributes = {"Elevation"};
        double[] testprob = {0.2};

        String imgName = "UTM";
        String testImgPath = "img/"+imgName+"-test2-3arr.png";
        String imgPath = "/Users/ltc/PycharmProjects/BinGraph/imageResource/" + imgName + "_binary.png";
        String wmImgPath = "img/"+imgName+"/Extract-" + imgName + "-Logo-2.png";
        String wmAndNoWmImgPath = "img/"+imgName+"/wmAndNoWm/"+"/WmAndNoWmImg-" + imgName +".png";
        String delTupleRemainWM = "img/"+imgName+"/delTuple/"+"/delTupleRemainWM-90%" + imgName + ".png";
        String delArrRemainWM = "img/"+imgName+"/delArr/"+"/delArrRemainWM-"+ 3 + "-Arr-" + imgName + ".png";

        int[][] rgbArray = GetImage2Array.getWatermark(imgPath);
        int width = rgbArray[0].length;    // 水印图片的宽度
        int height = rgbArray.length;      // 水印图片的高度
        int[][] ExtractWM;
        int[][] EmbedWMPos = new int[height][width];
        int[][] WmAndNoWmPos = new int[height][width];
        String tablename = "forest";
        int tuplestart = 1;
        int tupleLimit = 30000;
        int remainTupleNum = tupleLimit;
        String sk = "100101000111";
        int lsbSize = 1;
        int msbSize = 16;
        // float[] delProbs = {0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f};
        double[] probs = {0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3};
        double[] delProbs = {};
        String[] arrtributes = {"Elevation", "Aspect", "Slope", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm", "Horizontal_Fire_Points"};
        String[] testarrs = {"a1","a2","a3","a4","a5","a6","a7","a8","a9","a10"};
        String[] delArrs = {};

        // 生成VPK并嵌入水印
        // HashMap<String, Integer> origin_vpk = DataProcess2.genVPK4(probs, tablename, tupleLimit, sk, lsbSize, arrtributes, rgbArray, EmbedWMPos);     //在forest表中生成VPK
        // 查看图片水印，那些像素被嵌入，那些像素没有被嵌入，并输出图片
        // WmAndNoWmPos = DataProcess2.embedWMByVPK(rgbArray,EmbedWMPos,height,width);
        // GetImage2Array.writeImageFromArray(wmAndNoWmImgPath, "png", WmAndNoWmPos);

        // List<String> delArr_vpk = genVPK4(del_prob,"test","a2","a3","a4","a5","a6","a7","a8","a9","a10");

        System.out.println("我的方法结果：");
        HashMap<String, Integer> origin_vpk1 = DataProcess2.genVPK4(probs, tablename, tuplestart, tupleLimit, sk, lsbSize, arrtributes, rgbArray, EmbedWMPos, msbSize);     //在forest表中生成VPK
        DataProcess2.evaluateIndicator_One(origin_vpk1, tupleLimit, tuplestart);

        // 提取图片水印
//         ExtractWM = EmbedAndExtract.ExtractWM(probs, width, height, lsbSize, tablename, arrtributes, tuplestart, tupleLimit, sk, delArrs, remainTupleNum);
//         GetImage2Array.writeImageFromArray(testImgPath, "png", ExtractWM);
//
//         float cf = DataProcess2.evaluateIndicator_Two(rgbArray, ExtractWM, height, width);
//         System.out.println("图片水印提取正确的像素比：" + cf);

        // 判断删除属性后VPK的保持/损坏情况
        // delCompare(origin_vpk, delArr_vpk);

        // otherMethodExperiment();
    }
}
