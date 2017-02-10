package cn.junety.bloomfilter.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by caijt on 2017/2/9.
 * 读取 zip 包工具
 */
public class DataReader {

    public static List<String> readZip(String zipPath) throws IOException {
        FileInputStream fis = null;
        InputStream in = null;
        ZipInputStream zis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        List<String> items = new ArrayList<>();
        try {
            fis = new FileInputStream(zipPath);
            in = new BufferedInputStream(fis);
            zis = new ZipInputStream(in);
            isr = new InputStreamReader(zis);
            br = new BufferedReader(isr);

            ZipEntry entry;
            String str;
            //遍历所有文件及文件夹
            while ((entry = zis.getNextEntry()) != null) {
                if(entry.isDirectory()) continue;
                while((str = br.readLine()) != null) {
                    if(!"".equals(str.trim())) {
                        items.add(str);
                    }
                }
                break;
            }
        } finally {
            try {
                if (zis != null) {
                    zis.closeEntry();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            streamClose(br, isr, zis, in, fis);
        }
        return items;
    }

    private static void streamClose(Closeable... closeables) {
        if(closeables != null) {
            for(Closeable closeable : closeables) {
                try {
                    if(closeable != null) {
                        closeable.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
