package test.uploadDemo;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class UploadThread implements Runnable{
	
	private String localPath;
	private String hdfsPath;
	private long maxSplitSize;

	private long offset;
	private long offsetLength;
	private int fileIdx;
	private FileSystem fs;

	
	
	public long getOffsetLength() {
		return offsetLength;
	}

	public void setOffsetLength(long offsetLength) {
		this.offsetLength = offsetLength;
	}

	public UploadThread(FileSystem fs, String localPath, String hdfsPath, long maxSplitSize, long offset,
			long offsetLength, int fileIdx) {
		this.localPath=localPath;
		this.hdfsPath=hdfsPath;
		this.maxSplitSize=maxSplitSize;
		
		this.offset=offset;
		this.offsetLength=offsetLength;
		this.fileIdx=fileIdx;
		this.fs=fs;
	}

	public void run() {
		
		File file=new File(localPath);
		
		try {
			InputStream is = new BufferedInputStream(new FileInputStream(file));
		    byte b[]=new byte[(int) maxSplitSize];
		    String []fileNames=localPath.split("/");
		    String fileAllName=fileNames[fileNames.length-1];
		    
		    String fileName=fileAllName.split("\\.")[0];
		    String fileNickname="";
		    if(fileAllName.split("\\.").length>1) fileNickname=fileAllName.split("\\.")[1];
		    
		    try {
		    	System.out.println("线程 "+Thread.currentThread().getName()+"正在上传      起始偏移： "+" "+offset+" 切分长度：  "+offsetLength);
		    	is.skip(offset);
				is.read(b, 0, (int)offsetLength);
				
				InputStream in = new ByteArrayInputStream(b);
				
				//System.out.println(is.read(b, (int)offset, (int)offsetLength)+" "+in.read());
				String newPath=hdfsPath+"/"+fileName+"_part"+fileIdx;
				if(fileAllName.split("\\.").length>1) newPath+="."+fileNickname;
					
				//System.out.append(newPath);
				
				
				if(!fs.exists(new Path(newPath))) {
					fs.create(new Path(newPath));
				}
				
				OutputStream out = fs.create(new Path(newPath));

				
				IOUtils.copyBytes(in, out, offsetLength, false);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	
		
	}

}
