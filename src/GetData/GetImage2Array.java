package GetData;

import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;

public class GetImage2Array {
    public static void main(String[] args) {
        // 读取图片到BufferedImage
        BufferedImage bf = readImage("/Users/ltc/PycharmProjects/BinGraph/imageResource/WWF_binary.png");           //这里写你要读取的绝对路径+文件名
        // 将图片转换为二维数组
        int[][] rgbArray1 = convertImageToArray(bf);
        // 输出图片到指定文件
        writeImageFromArray("img/Test-UTM-Logo.png", "png", rgbArray1);//这里写你要输出的绝对路径+文件名
        System.out.println("图片输出完毕!");
    }

    /*
    读取图片文件到Buffer中
     */
    public static BufferedImage readImage(String imageFile){
        File file = new File(imageFile);
        BufferedImage bf = null;
        try {
            bf = ImageIO.read(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bf;
    }

    /*
    将图片转换为数组
     */
    public static int[][] convertImageToArray(BufferedImage bf) {
        // 获取图片宽度和高度
        int width = bf.getWidth();
        int height = bf.getHeight();
        // 将图片sRGB数据写入一维数组
        int[] data = new int[width*height];
        bf.getRGB(0, 0, width, height, data, 0, width);
        // 将一维数组转换为为二维数组
        int[][] rgbArray = new int[height][width];
        for(int i = 0; i < height; i++)
            for(int j = 0; j < width; j++)
                // 将图片的二维数组转换为01二值数组  0 黑色 1 白色
                if (data[i*width + j]==-1){
                    rgbArray[i][j] = 1;
                }else{
                    rgbArray[i][j] = 0;
                }
        return rgbArray;
    }
    public static void writeImageFromArray(String imageFile, String type, int[][] rgbArray){
        // 获取数组宽度和高度
        int width = rgbArray[0].length;
        int height = rgbArray.length;
        // 将二维数组转换为一维数组
        int[] data = new int[width*height];
        for(int i = 0; i < height; i++)
            for(int j = 0; j < width; j++)
                data[i*width + j] = rgbArray[i][j];
        // 将数据写入BufferedImage
        BufferedImage bf = new BufferedImage(width, height, BufferedImage.TYPE_INT_BGR);
        bf.setRGB(0, 0, width, height, data, 0, width);
        // 输出图片
        try {
            File file= new File(imageFile);
            ImageIO.write((RenderedImage)bf, type, file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 导入图片，变成水印，即01二值数组
     * @return
     */
    public static int[][] getWatermark(String imgPath){
        // 读取图片到BufferedImage
        BufferedImage bf = GetImage2Array.readImage(imgPath);           //这里写你要读取的绝对路径+文件名
        // 将图片转换为二维数组
        int[][] rgbArray1 = GetImage2Array.convertImageToArray(bf);
        return rgbArray1;
    }
}