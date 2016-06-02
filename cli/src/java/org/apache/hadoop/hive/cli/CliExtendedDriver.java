package org.apache.hadoop.hive.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;



public class CliExtendedDriver {
	static List<String> parseResult=new ArrayList<String>();
	static String result="";
	
	static boolean DEBUG=false;
	
	
	public static String matchPatternGen(){
		StringBuilder sb=new StringBuilder();
		String sep="[ ]{1,}";
		String linesep="[ ]{0,}[\\n]{0,1}[ ]{0,}";
		
		String[] keywords={
				"Create", "(Soft|Hard)", "Correlation", "(\\w+)", "nextLine",
				"On","(\\w+)","nextLine",
				"SourceAttrIndex", "(\\d+)","nextLine",
				"DestinationAttrIndex","(\\d+)", "nextLine",
				"Granularity", "(Singleton|Range|List)", "nextLine",
				"MappingFunc", "(\\w+)","nextLine",
				"InputDataType","(Text|Integer|Double|Float)","nextLine",
				"OutputDataType","(Text|Integer|Double|Float)", "nextLine",
				"[;]{0,1}"
				
		};
		
		for(int i=0;i<keywords.length;i++){
			if(i==0){
				sb.append(keywords[i]);
			}else{
				if(keywords[i].compareTo("nextLine")==0){
					continue;
				}
				if(keywords[i-1].compareTo("nextLine")!=0){
					sb.append(sep);
					sb.append(keywords[i]);
				}else{
					sb.append(linesep);
					sb.append(keywords[i]);
				}
			}
		}
		
		//
		
		return sb.toString();
	}
	
	public static int processCmd(String cmd){
		
		parseResult.clear();
		result="";

		String pString=matchPatternGen();
		
		Pattern p=Pattern.compile(pString);
		Matcher m=p.matcher(cmd);
		
		while(m.find()){
			//parseResult.add(m.group());
			for(int i=1;i<10;i++){
				parseResult.add(m.group(i));
				if(DEBUG){
					System.out.printf("presult:%s\n", m.group(i)); 
				}
			}
		}
	
		
		
		return 0;
	}
	
	
	public  int refactorCmd(String cmd){
		
		//System.out.printf("refactorCmd:%s \n",cmd);

		processCmd(cmd);

		if(parseResult.isEmpty()){
			return -1; 
		}
		


		if((parseResult.get(0)).equals("Soft")){
			String queryFormat="INSERT INTO SHCorrelation (SHC_TYPE, SHC_NAME, SHC_TNAME, SHC_SID, SHC_DID , SHC_GRAN, "
					+ "SHC_MAPFUNC,SHC_INPUT_TYPE, SHC_OUTPUT_TYPE, SHC_STATUS) \n"
					+"Values \n "
					+"(\'%s\',\'%s\',\'%s\',%s, %s,\'%s\', \'%s\', \'%s\', \'%s\','V');";
			result=String.format(queryFormat, parseResult.toArray());
		}
		else{
			String queryFormat="INSERT INTO SHCorrelation (SHC_TYPE, SHC_NAME, SHC_TNAME, SHC_SID, SHC_DID , SHC_GRAN, "
					+ "SHC_MAPFUNC,SHC_INPUT_TYPE, SHC_OUTPUT_TYPE, SHC_STATUS) \n"
					+"Values \n "
					+"(\'%s\',\'%s\',\'%s\',%s, %s,\'%s\', \'%s\', \'%s\', \'%s\', 'D');";
		
			result=String.format(queryFormat, parseResult.toArray());
		}
		
		
	
		
		
		return 0;
	}

	
	
	
	public static void run() throws Exception{
		
		Connection conn = null;
        try {

        	

            HiveConf conf = new HiveConf();
           
           	String homePath=System.getenv("HIVE_HOME");
           	//System.out.printf("hive home setting:%s\n", homePath); 
            conf.addResource(new Path(homePath+"/conf/hive-site.xml"));
            Class.forName(conf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
            conn = DriverManager.getConnection(
                    conf.getVar(ConfVars.METASTORECONNECTURLKEY),
                    conf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
                    conf.getVar(ConfVars.METASTOREPWD));

            Statement st = conn.createStatement();
            if(DEBUG){
            	System.out.printf("exe:%s\n", result);
            }
            int rs = st.executeUpdate(
            	result
                );

            System.out.printf("Query OK, %d row in Metastore.sharc affected \n",rs);
            System.out.printf("Create Correlation, please provide your java/python script to %s \n",homePath+"/userScripts/"); 
          	
        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }

	}
	


	
	// public static void main(String[] args) {
 //       /// System.out.println("Hello World!"); // Display the string.
		
		
		
	// 	// if(DEBUG){
	// 	// 	String testCmd="Create Soft Correlation tableName \n"
	// 	// 			+"On Hive_tableName \n"
	// 	// 			+"SourceAttrIndex 1 \n"
	// 	// 			+"DestinationAttrIndex 2 \n"
	// 	// 			+"Granularity Singleton \n"
	// 	// 			+"MappingFunc mapp_func \n"
	// 	// 			+"InputDataType Integer \n"
	// 	// 			+"OutputDataType Integer \n"
	// 	// 			+";";
			
	// 	// 	processCmd(testCmd);
	// 	// 	refactorCmd();
	// 	// 	System.out.printf("%s\n",result);
	// 	// }
	// 	// int ret = new CliDriver().run(args);
		
		
 //    }
}