package Focus;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * 获取Windows聚焦壁纸方法
 * @ClassName getFocusUtil
 * @Description TODO
 * @Author Winter
 * @Date 2018/11/13 17:25
 **/
public class getFocusUtil {

    /**
     * 重命名windows屏保图片文件
     * @param DocPath	Windows屏保路径
     * @param OutPath	导出路径
     */
    public static void ReNameFile(String DocPath,String OutPath){
        String fileName = "";
        String newFileName = "";
        File file = null;
        file = new File(DocPath);
        if(!file.isDirectory()){
            System.out.println("文件");
        }else if(file.isDirectory()){
            System.out.println("文件夹");
            File[] files =  file.listFiles();
            for(File tmpFile : files){
                if(tmpFile.isFile()){
                    fileName = tmpFile.getName(); //当前文件名
                    newFileName = fileName+"_new.jpg";
                    renameFile(DocPath,OutPath, fileName, newFileName);
                }
            }
        }
    }

    /**文件重命名
     * @param path 文件目录
     * @param oldname  原来的文件名
     * @param newname 新文件名
     */
    /**
     * 文件重命名
     * @param path	原文件路径
     * @param outPath	导出文件路径
     * @param oldname	原文件名
     * @param newname	新文件名
     */
    public static void renameFile(String path,String outPath,String oldname,String newname){
        if(!oldname.equals(newname)){//新的文件名和以前文件名不同时,才有必要进行重命名
            File oldfile=new File(path+"\\"+oldname);
            File newfile=new File(outPath+"\\"+newname);
            File dir = newfile.getParentFile(); //获取文件夹
            if (!dir.exists()) { //如果导出文件夹不存在，则创建
                dir.mkdirs();
            }
            if(!oldfile.exists()){
                return;//重命名文件不存在
            }
            if(newfile.exists())//若在该目录下已经有一个文件和新文件名相同，则不允许重命名
                System.out.println(newname+"已经存在！");
            else{
                try {
                    //复制到导出路径，原路径下文件依然存在
                    FileUtils.copyFile(oldfile,newfile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //下面的方式为剪切方式，剪切后，原路径下没有图片文件
                //oldfile.renameTo(newfile);
            }
        }else{
            System.out.println("新文件名和旧文件名相同...");
        }
    }

    public static void main(String[] args) {
        //使用时，将Winter换为自己的用户名
        getFocusUtil.ReNameFile("C:\\Users\\Winter\\AppData\\Local\\Packages\\Microsoft.Windows.ContentDeliveryManager_cw5n1h2txyewy\\LocalState\\Assets","C:\\focus\\1");
    }
}
