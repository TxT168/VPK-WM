package Vpk;

import java.util.Arrays;
import java.util.Random;
import java.lang.Integer;

public class TestIntToBinary {
    public static void main(String[] args) {
        int[] data = {16868,24678,9099,737281};             // 测试的原始数据
        int strlen;                                         // 数据转换为二进制字符串的长度
        char[] charStr;                                     // 转换为的二进制字符数组
        int msb_pos;                                        // 单个属性最终选择的msb位置
        char[] bj = {'1','1','0'};                          // 每个元组的初始bj
        String sk = "370183306";                            // 初始密钥
        for (int i = 0; i < data.length; i++) {
            int tupledata = data[i];
            if (tupledata<64 && tupledata>-65) continue;
            // 每个属性最终选择的MSB位置
            // 数值转换为二进制字节数组
            String binStr = Integer.toBinaryString(tupledata);
            strlen = binStr.length();
            System.out.println("the length is : " + strlen);
            charStr = binStr.toCharArray();
            System.out.println("原来的二进制：" + binStr);

            // 判断单个属性的MSB位置 和 是否会被用于构建虚拟主键
            msb_pos = msbSelector(charStr, strlen);
            System.out.println("该属性最后MSB的位置：" + msb_pos);
            boolean isArr = isArrSelected(charStr, msb_pos);
            System.out.println("该属性是否被选择：" + isArr);
            if (isArr){
                bj = tupleVPK(charStr, msb_pos, bj);
            }
        }

        String bjStr = String.valueOf(bj);
        System.out.println("改变后的二进制：" + bjStr);
        long bjData = Long.parseLong(bjStr, 2);
        System.out.println(bjData);
        String conStr = sk + bjData;
        System.out.println("合并后的十进制：" + conStr);
        long finData = Long.parseLong(conStr, 10);
        System.out.println("改变后的十进制数字：" + finData);


        // 构建单个元组的虚拟主键


        //test();
    }

//    // int到byte[]
//    public static byte[] intToByteArray(int i) {
//        byte[] result = new byte[4];
//        result[0] = (byte)((i >> 24) & 0xFF);
//        result[1] = (byte)((i >> 16) & 0xFF);
//        result[2] = (byte)((i >> 8) & 0xFF);
//        result[3] = (byte)(i & 0xFF);
//        return result;
//    }
//
//    // byte[]转int
//    public static int byteArrayToInt(byte[] bytes) {
//        int value=0;
//        for(int i = 0; i < 4; i++) {
//            int num= (3-i) * 8;
//            value +=(bytes[i] & 0xFF) << num;
//        }
//        return value;
//    }

    /**
     * 单个属性上随机选择具体的MSB位置
     */
    public static int msbSelector(char[] charStr, int strlen){
        Random random = new Random();
        int ran_pos = random.nextInt(strlen/2);
        System.out.println("生成随机数：" + ran_pos);
        if (charStr[ran_pos+1]=='0') {
            if (charStr[ran_pos+2]=='0') {
                return ran_pos;
            } else {
                return ran_pos-1;
            }
        } else {
            if (charStr[ran_pos+2]=='0') {
                return ran_pos;
            } else {
                return ran_pos+1;
            }
        }
    }

    /**
     * 判断单个属性是否用作构建虚拟主键
     */
    public static boolean isArrSelected(char[] charStr, int pos){
        return (charStr[1] + charStr[pos]) == '1' + '0';
    }

    /**
     * 单个元组构建虚拟主键
     */
    public static char[] tupleVPK(char[] charStr, int pos, char[] bj) {
        int bjlen = bj.length;
        int len = pos + bjlen;
        char[] conMSB = new char[len];
        for (int i=0; i<bjlen; i++) {
            conMSB[i] = bj[i];
        }
        for (int j=bjlen; j<len; j++) {
            conMSB[j] = charStr[j - bjlen];
        }
        return conMSB;
    }

    public static void test(){
        char[] n = {'1','0','1','1','0','0','1','0'};
        String changeStr = String.valueOf(Arrays.copyOfRange(n, 0, 3));
        System.out.println("改变后的二进制：" + changeStr);
//        int changeData = Integer.parseInt(changeStr, 2);
//        System.out.println("改变后的数据：" + changeData);
//        System.out.println((int));
    }

}
