package Vpk;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DataProcess4 {
    /**
     * 将一个属性的所有值转换为MSB二进制字符串，并存储在一个hashmap中
     * @param arrName
     * @param tablename
     * @param lsbSize
     * @param sk
     * @return
     * @throws SQLException
     */
    public static HashMap<Integer, char[]> arrSetToMSBHashMap(String arrName, String tablename, int lsbSize, String sk, int tupleSum) {
        int curTupleCount = 0;  //记录当前元组的总数
        HashMap<Integer, char[]> curArrHashMap = new HashMap<>();

        //创建连接
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select " + arrName + " from " + tablename + " LIMIT " + tupleSum;
        try{
            //绑定sql语句
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs=pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while(rs.next()) {
                curTupleCount++;            //统计使用的元组数
                String arrBinStrMSB = null;        // 截取每个属性值二进制串的最大有效位 msb

                String curArrType = metaData.getColumnTypeName(1);
                String tempStr = null;
                if (curArrType == "INT") {
                    tempStr = Integer.toBinaryString(rs.getInt(1));
                } else if (curArrType == "FLOAT") {
                    int temp2 = Float.floatToIntBits(rs.getFloat(1));
                    tempStr = Integer.toBinaryString(temp2);
                } else if (curArrType == "DOUBLE") {
                    long temp3 = Double.doubleToLongBits(rs.getDouble(1));
                    tempStr = Long.toBinaryString(temp3);
                }

                // 截取每个属性值二进制串的最大有效位 msb
                int tempStrLen = tempStr.length();

                if (tempStrLen > lsbSize) {
                    arrBinStrMSB = tempStr.substring(0, tempStrLen - lsbSize);
                    char[] charHashStrMSB = DataProcess2.testencryptMD5(sk.concat(arrBinStrMSB)).toCharArray();
                    curArrHashMap.put(curTupleCount, charHashStrMSB);
                } else {
                    char[] charHashStrMSB = {'N'};
                    curArrHashMap.put(curTupleCount, charHashStrMSB);    // MSB不够比特位，所以字符串赋值为 N
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            JDBCUtil.DBClose(con, pstmt, rs);
        }
        return curArrHashMap;
    }

    public static HashMap<Integer, char[]> selectHashStringToProb(double arrProb, HashMap<Integer, char[]> curArrHashMap, int tupleSum){
        double prob = arrProb;
        int needArrNum = (int)(tupleSum * arrProb);
        char[] arrProbBin = probToBinByte(prob);
        String final_probBinStr = "";
        int probLen = arrProbBin.length;                 //属性的概率字节数组的长度

        List<Integer> recordList = new ArrayList<>();       // 记录当前符合比例的属性值的index
        // 判断当前比例下满足要求的元组数是否超过要求的个数，超过进入下一步
        // 没有超过的话，调高比例，直到满足的个数超过要求的个数
        boolean meetFlag = false;
        int arrCurPos = 0;              //最开始从最右边开始，然后以实时调整比例的二进制串长度为步长
        int countMeetArrValue = 0;                 // 统计满足条件的属性值的个数
        while (!meetFlag){
            for (int key : curArrHashMap.keySet()) {
                char[] charHashStrMSB = curArrHashMap.get(key);
                int charHashStrLen = charHashStrMSB.length;

                for (int j = 1; j <= probLen; j++) {
                    // 判断是否超出了属性值二进制Hash串的长度，超出就跳过该属性值
                    if (charHashStrLen-j-arrCurPos<0){
                        break;
                    }
                    char curCharHash = charHashStrMSB[charHashStrLen-j-arrCurPos];

                    if (curCharHash=='0'){
                        continue;
                    }else if (curCharHash=='1' && arrProbBin[j-1]=='1') {
                        recordList.add(key);
                        break;
                    }else if (curCharHash=='1' && arrProbBin[j-1]=='0') {
                        break;
                    }
                }
            }

            // 遍历所有的属性值，查看当前满足的比例是否在误差范围内，不管是多了还是少了
            countMeetArrValue = recordList.size();
            //System.out.println("当前所选的属性数有：" + countMeetArrValue);
            if (Math.abs(countMeetArrValue - needArrNum) <= (float)(tupleSum*0.01)){
                meetFlag = true;
                // 删减hashmap中不符合当前比例的数据
                Set<Integer> tempKeySet = curArrHashMap.keySet();
                tempKeySet.retainAll(recordList);

                final_probBinStr = final_probBinStr.concat(String.copyValueOf(arrProbBin));
            }else{
                // 误差范围外，如果已经超过要求的个数，继续加入下一个比例往下走；
                if (countMeetArrValue > needArrNum){
                    // 更新下一次比例匹配的位置
                    arrCurPos = arrCurPos + probLen;
                    // 录入当前的比例二进制串到final_String中
                    final_probBinStr = final_probBinStr.concat(String.copyValueOf(arrProbBin));
                    //System.out.println(final_probBinStr);
                    // 更新下一个比例
                    float tempNewProb = needArrNum / (float)countMeetArrValue;
                    arrProbBin = probToBinByte(tempNewProb);
                    probLen = arrProbBin.length;

                    // 删减hashmap中不符合当前比例的数据
                    Set<Integer> tempKeySet = curArrHashMap.keySet();
                    tempKeySet.retainAll(recordList);

                    // 重新清空recordList，在下一个比例中继续做记录
                    recordList.clear();

                    // 更新下一次的开始位置
                    if (arrCurPos+probLen>128){
                        System.out.println("haha");
                        break;
                    }

                }else {
                    // 误差之外，当前比例下，没有超过要求的个数，将当前的比例调高，直到超过要求的个数或者直接就满足比例要求
                    prob = prob + (needArrNum-countMeetArrValue) / (float)needArrNum;
                    arrProbBin = probToBinByte(prob);
                    probLen = arrProbBin.length;
                    // 重新清空recordList，在下一个比例中继续做记录
                    recordList.clear();
                }

            }
        }
        System.out.println("最终被选择的属性个数有：" + countMeetArrValue + "    比例：" + (countMeetArrValue/(float)tupleSum));
        System.out.println("最终的选择的二进制字符串为：" + final_probBinStr);
        System.out.println("字符串长度为" + final_probBinStr.length());
        return curArrHashMap;

    }

    /**
     * 比例转换为二进制串
     * 方法1：借用java内部的方法，但是出来的精度有限
     * @param prob
     * @return
     */
    public static char[] probToBinByte(double prob) {
        long n = Double.doubleToLongBits(prob);
        String binStr = Long.toBinaryString(n);
        String exponent = "";    //阶码
        String fraction = "";    //尾数
        int pos = 32 - binStr.length();
        int subPos = 0;
        for (; pos < 9; subPos++, pos++) {
        }
        exponent = binStr.substring(0, subPos);
        fraction = binStr.substring(subPos);
        fraction = "1".concat(fraction);
        char[] charFrac = fraction.toCharArray();
        int intExp = 127 - Integer.parseUnsignedInt(exponent, 2);

        for (int i = fraction.length() - 1; i >= 0; i--) {
            if (charFrac[i] == '1') {
                fraction = fraction.substring(0, i + 1);
                break;
            }
        }

        for (int i = 0; i < intExp - 1; i++) {
            fraction = "0".concat(fraction);
        }
        //System.out.println(fraction);         //打印转换后得到的二进制字符串
        if (fraction.length()>16){
            fraction = fraction.substring(0,16);
        }
        return fraction.toCharArray();
    }

    public static char[] probToBinByte2(double prob) {
        StringBuffer stringBuffer = new StringBuffer();
        String probBinString;
        int probBinStringLen = 128;        // 最大的拟合二进制串的长度为128位
        double sum = 0;
        for (int i = 1; i <= probBinStringLen; i++) {
            double temp = sum + Math.pow(0.5, i);
            if (temp > prob){
                stringBuffer.append("0");
            }else if(prob > temp){
                stringBuffer.append("1");
                sum = temp;
            }else {
                stringBuffer.append("1");
                break;
            }
        }
        probBinString = stringBuffer.toString();
        char[] binStringChars = probBinString.toCharArray();
        System.out.println("最终的二进制拟合字符串：" + probBinString);
        return binStringChars;
    }

    public static HashMap<Integer, String> getFinalVPK(double[] probs, String[] arrNames, int tupleSum, String tablename, String sk){
        int len = probs.length;
        HashMap<Integer, String> vpk = new HashMap<>();
        for (int i = 0; i < len; i++) {
            String curArrName = arrNames[i];
            HashMap<Integer, char[]> curArrHashMap = arrSetToMSBHashMap(curArrName, tablename, 3, sk, tupleSum);
            double curArrProb = probs[i];
            HashMap<Integer, char[]> arrHashMap = selectHashStringToProb(curArrProb, curArrHashMap, tupleSum);
            for (Integer key : arrHashMap.keySet()){
                String tempVpkArrHashBinStr = vpk.get(key);
                String arrHashBinStr = String.copyValueOf(arrHashMap.get(key));
                if (tempVpkArrHashBinStr != null) {
                    tempVpkArrHashBinStr = tempVpkArrHashBinStr.concat(arrHashBinStr);
                    vpk.put(key, tempVpkArrHashBinStr);
                }else{
                    vpk.put(key, arrHashBinStr);
                }
            }
            for (Integer vpkKey : vpk.keySet()){
                String finalOneVPK = DataProcess2.testencryptMD5(sk.concat(vpk.get(vpkKey)));
                vpk.put(vpkKey, finalOneVPK);
            }
        }
        return vpk;
    }

    public static HashMap<String, Integer> countVpkRepeat(HashMap<Integer, String> final_VPK){
        HashMap<String, Integer> hashMap = new HashMap<>();
        for (Integer key : final_VPK.keySet()){
            String oneVpk = final_VPK.get(key);
            if (hashMap.get(oneVpk) != null) {
                Integer value = hashMap.get(oneVpk);
                hashMap.put(oneVpk, value+1);
            } else {
                hashMap.put(oneVpk, 1);
            }
        }
        return hashMap;
    }

    public static void evaluateIndicator_One(HashMap<String, Integer> vpk, int tupleLimit){
        Set<String> vpkSet = vpk.keySet();
        int exclusiveNum = 0;
        float duplicateGroupFraction = 0;
        float p = 0;
        int count = 0;
        int max = 0;
        for (String vpkstr : vpkSet){
            int temp = vpk.get(vpkstr);
            if (temp>max){
                max = temp;
            }
            if (temp==1){
                exclusiveNum++;
                //System.out.println(vpkstr);
            }else{
                count++;
                duplicateGroupFraction = duplicateGroupFraction + 1.0f / temp;
            }
        }
        p = (duplicateGroupFraction+(float)exclusiveNum) * 100 / tupleLimit;

        System.out.println("VPK排他值：" + exclusiveNum);
        System.out.println("Group数：" + count);
        System.out.println("Group中最大size为：" + max);
        System.out.println("指标p：" + p);
    }


    public static void test(){
        String str = "nihao";
        int a = 2;
        int b = 10;
//        HashMap<Integer, String> hashMap = new HashMap<>();
//        hashMap.put(1,"nihao");
//        hashMap.put(2, "nihao");
//        System.out.println(hashMap);
//        System.out.println("删除");
//        hashMap.remove(2);
//        System.out.println(hashMap);
        //System.out.println(a/(float)b);
        //System.out.println(DataProcess2.testencryptMD5(str).length());
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(3);
        List<Integer> list2 = new ArrayList<>();
        list2.add(2);
        list2.add(1);
        list2.add(3);
        list2.retainAll(list);
        System.out.println(list2);

    }

    public static void main(String[] args) throws SQLException {
        String tablename = "forest";
        String sk = "100101000111";
        int tupleSum = 30000;
        double[] prob = {0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3};
        String[] arrtributes = {"Elevation", "Aspect", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_3pm", "Horizontal_Fire_Points"};
        // 测试
        float[] prob2 = {0.01f};
        String[] arrtributes2 = {"Elevation"};
        HashMap<Integer, String> final_VPK = getFinalVPK(prob, arrtributes, tupleSum, tablename, sk);
        HashMap<String, Integer> countVpkRe = countVpkRepeat(final_VPK);
        evaluateIndicator_One(countVpkRe, tupleSum);


    }

}
