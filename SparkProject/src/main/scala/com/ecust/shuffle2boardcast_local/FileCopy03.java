package com.ecust.shuffle2boardcast_local;

import java.io.*;

public class FileCopy03 {
    public static void main(String[] args) throws IOException {
        //从哪拷？ 源目录
        //拷到哪？目标目录
        //怎么拷？ 调用方法

        File srcFile=new File("D:/123");
        File destFile=new File("D:/new");
        copyDir(srcFile,destFile);
    }

    private static void copyDir(File srcFile, File destFile) throws IOException {

        if(srcFile.isFile()){
            //是文件，就一边读一边写
            FileInputStream in=null;
            FileOutputStream out=null;

            try {
                in=new FileInputStream(srcFile);
               
                String path=destFile.getAbsolutePath().endsWith("\\")?destFile.getAbsolutePath():destFile.getAbsolutePath()+"\\"+srcFile.getAbsolutePath().substring(3);
                out=new FileOutputStream(path);
                byte[] bytes=new byte[1024*9];
                int readCount=0;
                while((readCount=in.read(bytes))!=-1){

                    out.write(bytes,0,readCount);
                }
                out.flush();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }


        //获取源目录下的所有子目录
        File[] files= srcFile.listFiles();
        //遍历files
        for(File file:files){
            //如果是目录，拿到每个目录的绝对路径
            if(srcFile.isDirectory()){
                String srcDir=file.getAbsolutePath();
                //然后把绝对路径去掉盘符(从3开始读)，拼接到目标目录中，就拿到了目标目录中子目录的绝对路径
                String destDir=(destFile.getAbsolutePath().endsWith("\\")?destFile.getAbsolutePath():destFile.getAbsolutePath()+"\\")+srcDir.substring(3);
                //在目标目录的绝对路径下创建新的File
                File newfile = new File(destDir);
                if(!newfile.exists()){
                    newfile.mkdirs();
                }
            }
            copyDir(file,destFile);
        }

    }
}
