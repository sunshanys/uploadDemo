package test.uploadDemo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.security.UserGroupInformation;


public class App 
{
	
	static List<UploadThread> treads = new ArrayList<UploadThread>();
	static Configuration conf = new Configuration();
	static String localPath;
	static String hdfsPath;
	static int threadNum;
	static int maxSplitSize = 1;
	static String maxSplitSizeStr;
	static String coreSitePath;
	static String hdfsSitePath;
	static String krbPath;
	static String krbConf;
	static String krbXML;
	
	public static void main(String[] args) throws Exception {
		setProperties();
		conf.addResource(new Path(coreSitePath));
		conf.addResource(new Path(hdfsSitePath));
		isKrbLogin();
		String hdfsIP = getActiveNamenode();
		String hdfsTPath = "hdfs://" + hdfsIP + hdfsPath;
		URI uri = new URI("hdfs://" + hdfsIP);
		FileSystem fs = FileSystem.get(uri, conf);
		String basePath=localPath.trim();
		isFileORDir(fs,hdfsTPath,basePath);
		startUpload();
		fs.close();
	}
	
    /**
     * 判断本地路径是文件还是文件夹
     * @param localFile
     * @param fs
     * @param hdfsTPath
     */
	private static void isFileORDir(FileSystem fs, String hdfsTPath ,String basePath) {
		File localFile = new File(basePath);
		if (!localFile.isDirectory()) {
			preUpload(fs, basePath, hdfsTPath.substring(0, (hdfsTPath.lastIndexOf("/")==-1?hdfsTPath.length():hdfsTPath.lastIndexOf("/"))), maxSplitSize);
		} else {
			for (String fileName : localFile.list()) {
				//递归调用，防止目录中存在子目录
				isFileORDir(fs, hdfsTPath + "/" + fileName,basePath+ "/" + fileName);
			}
		}

	}

	/**
     * 是否需要安全登陆
     * @param fi
     * @throws IOException
     */
	private static void isKrbLogin() throws IOException {
		File fi = new File(krbPath);
		if (fi.exists()) {		
			System.setProperty("java.security.krb5.conf", krbConf);
			//读取认证文件信息
			conf.addResource(new Path(krbXML));
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			conf.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());
			conf.setBoolean("hadoop.security.authentication", true);
			conf.set("hadoop.security.authentication", "kerberos");
			//现改为从配置文件读取规则
			conf.set("dfs.namenode.kerberos.principal", conf.get("guardian.client.principal"));
			conf.set("dfs.datanode.kerberos.principal", conf.get("guardian.client.principal"));
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(conf.get("guardian.client.principal"), krbPath);
		}

	}

	/**
     * 获取配置文件中参数
     * @throws IOException 
     * @throws FileNotFoundException 
     */
	private static void setProperties() throws FileNotFoundException, IOException {

		Properties p = new Properties();
		System.out.println(App.class.getResource("").toString());
		//配置文件路径
		p.load(new FileInputStream(new File("/root/demoTask/upload.properties")));
		localPath = p.getProperty("localPath");
		hdfsPath = p.getProperty("hdfsPath");
		threadNum = Integer.valueOf(p.getProperty("threadNum"));
		// String pressType=p.getProperty("pressType");
		maxSplitSizeStr = p.getProperty("MaxSplitSize");
		coreSitePath = p.getProperty("coreSite");
		hdfsSitePath = p.getProperty("hdfsSite");
		krbPath = p.getProperty("krbPath");
		krbConf = p.getProperty("krbConf");
		krbXML = p.getProperty("krbXML");
		for (String size : maxSplitSizeStr.split("\\*")) {
			maxSplitSize *= Integer.valueOf(size.trim());
		}
		

	}

	/**
     * 开启指定数量线程任务
     * @param threadNum
     */
	private static void startUpload() {
		ExecutorService service = Executors.newFixedThreadPool(threadNum);
		for (UploadThread runThread : treads) {
			service.execute(runThread);
		}
		service.shutdown();
		while (!service.isTerminated()) {
		}

	}

	/**
     * 根据配置文件获取activeNode
     * @return ip:port
     * @throws Exception
     */
	private static String getActiveNamenode() throws Exception {
		FileSystem fs = FileSystem.get(conf);
		InetSocketAddress active = HAUtil.getAddressOfActive(fs);
		// System.out.println(active.getAddress().getHostAddress()+"
		// "+active.getHostName().toLowerCase(Locale.getDefault())+"
		// "+active.getPort());
		return active.getAddress().getHostAddress() + ":" + active.getPort();
	}
    
	/**
	 * 将需要上传的文件按大小进行切分，并将信息保存在线程实例中
	 * @param fs
	 * @param localPath
	 * @param hdfsPath
	 * @param maxSplitSize
	 */
	private static void preUpload(FileSystem fs, String localPath, String hdfsPath, long maxSplitSize) {
		long fLen = new File(localPath).length();
		int splitNum = (int) (fLen / maxSplitSize);
		if (fLen % maxSplitSize != 0)
			splitNum += 1;
		long offset = 0;
		long offsetLength = maxSplitSize;
		int fileIdx = 0;
		for (int i = 0; i < splitNum; i++) {
			if (i == splitNum - 1)
				offsetLength = fLen % maxSplitSize;
			UploadThread upThread = new UploadThread(fs, localPath, hdfsPath, maxSplitSize, offset, offsetLength,
					fileIdx);
			treads.add(upThread);
			offset += offsetLength;
			fileIdx += 1;
		}

	}
}
