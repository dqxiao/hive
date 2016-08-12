package org.apache.hadoop.hive.cli;

import java.awt.List;
import java.util.ArrayList;

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.IExpressionVisitor;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TParseTreeNode;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;



/**
 *
 * Query Rewriting 
 *
 */
class ExprVisitor implements IExpressionVisitor {
	private static boolean hasEquals=false;

	public boolean hasEqualExpre(){
		return hasEquals;
	}
	
	@Override
	public boolean exprVisit(TParseTreeNode pNode, boolean isLeafNode) {
		// TODO Auto-generated method stub
			if(!isLeafNode){
			
				if(pNode instanceof  TExpression){
					TExpression  subExpr=(TExpression)pNode;
					switch(subExpr.getExpressionType()){
					case simple_comparison_t:
						if(subExpr.getComparisonType().toString().equals("equals")){
							
							if(subExpr.getRightOperand().getExpressionType().toString().equals("simple_constant_t")){
								hasEquals=true;
							}
						}
						
					default:
						break;
					}
					
					//System.out.printf("%s \n", subExpr.getExpressionType());
				}
			}
			
	
		return true;
	};
}



/*
* for setting source val for each relation  
* based on the equal expression 
*/
class ExprVisitorExtract implements IExpressionVisitor {
	
	TableMetadata tmeta; 
	ArrayList<Correlation> correlations=new ArrayList<Correlation>();
	
	public ExprVisitorExtract(TableMetadata t, ArrayList<Correlation>  corrs){
		tmeta=t;
		correlations=corrs;
	}
	
	public ArrayList<Correlation> getCorrelations(){
		return correlations;
	}

	@Override
	public boolean exprVisit(TParseTreeNode pNode, boolean isLeafNode) {
		// TODO Auto-generated method stub
		if(!isLeafNode){
		
			
			if(pNode instanceof  TExpression){
				TExpression  subExpr=(TExpression)pNode;
				switch(subExpr.getExpressionType()){
				case simple_comparison_t:
					//System.out.printf("%s \n", subExpr.getComparisonType());
					if(subExpr.getComparisonType().toString().equals("equals")){
					
						String sourceAttr= subExpr.getLeftOperand().toString();
						String sourceVal=subExpr.getRightOperand().toString();
						
						for(Correlation cor: correlations){
							if(tmeta.getColumnNameByIndex(cor.SHC_SID).equals(sourceAttr)){
								TExpression rightExp=subExpr.getRightOperand();
								cor.setSourceVal(rightExp.toString());
							}
						}
					
						
					}
					break;
				default:
					break;
				}
				
				//System.out.printf("%s \n", subExpr.getExpressionType());
			}
		}
		

		return true;
	}
	
}



/*
* Used for travel the parse tree
*/
class ExprVisitorRefactor implements IExpressionVisitor {
	
	TableMetadata tmeta;
	ArrayList<Correlation> correlations=new ArrayList<Correlation>();


	String orignalExp=""; // done 

	
	public ExprVisitorRefactor(TableMetadata t, ArrayList<Correlation>  corrs){
		tmeta=t;
		correlations=corrs;
	}
	
	public ArrayList<Correlation> getCorrelations(){
		return correlations;
	}

	public String getOrgExp(){
		return orignalExp;
	}

	@Override
	public boolean exprVisit(TParseTreeNode pNode, boolean isLeafNode) {
		// TODO Auto-generated method stub
		if(!isLeafNode){
			
			//System.out.printf("%s\n",pNode.toString());
			
			if(pNode instanceof  TExpression){
				TExpression  subExpr=(TExpression)pNode;
				switch(subExpr.getExpressionType()){
				case simple_comparison_t:
					
					// done 
					String orgExp=subExpr.toString();
					orignalExp=orgExp;

					if(subExpr.getComparisonType().toString().equals("equals")){
					
						String sourceAttr= subExpr.getLeftOperand().toString();
						String sourceVal=subExpr.getRightOperand().toString();
						
						for(Correlation cor: correlations){
			
							if(cor.isNullTval()){
								continue;
							}	
							if(tmeta.getColumnNameByIndex(cor.SHC_SID).equals(sourceAttr)){
//								System.out.printf("info:%s \n", "SHC_ID get");
								TExpression leftExp=subExpr.getLeftOperand();
								leftExp.setString(tmeta.getColumnNameByIndex(cor.SHC_DID));
								subExpr.setLeftOperand(leftExp);
								
								TExpression inExp=subExpr.getBetweenOperand(); 
								switch(cor.getGranType()){
									case Range:
										inExp.setString("between");
										break;
									case List:
										inExp.setString("in");
										break;
									default:
										break;
								}
								
								subExpr.setBetweenOperand(inExp);
								
								TExpression rightExp=subExpr.getRightOperand();
								rightExp.setString(cor.SHC_TargetVal);
								
								subExpr.setRightOperand(rightExp);
								// done  
							}
						}
						String newExp=subExpr.toString();

						String expString=String.format("%s and %s", orgExp, newExp);
					
						subExpr.setString(expString);
						
					}
					break;
				default:
					break;
				}
				
			}
		}
		

		return true;
	}
	
}

public class HiveQueryParserEx {
	static TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
	private static String tableName;
	private static TableMetadata meta;
	private static ArrayList<Correlation> correlations;
	private static String result="";

	private static boolean debug=false;
	private static boolean log=false;

	public HiveQueryParserEx(){
		
		tableName="";
		meta=new TableMetadata();
		correlations =new ArrayList<Correlation>();
		result="";
	}
	
	
	
	
	
	//done 
	
	private static boolean hasEqualExpr(TSelectSqlStatement select){

		if(select.getWhereClause()==null){
			return false;
		}
		TExpression expr = select.getWhereClause().getCondition();
		
		//System.out.println("pre order");
		
		ExprVisitor visitor=new ExprVisitor();
        expr.postOrderTraverse(visitor);
       
        return visitor.hasEqualExpre();
	}
	
	

	private static boolean singleTabelStmt(TSelectSqlStatement stmt){
		int count=stmt.tables.size();
		
		if(count==1){
			tableName=stmt.tables.getTable(0).getTableName().toString();
			if(debug){
				System.out.printf("related table: %s \n", tableName);
			}
			return true;
		}
		
		return false;
	}
	
	public static int parse(String cmd){
		
		 sqlparser.sqltext=cmd;
		 int i=sqlparser.parse();
		 int result=-1;
		 
		
		 if(i==0){
			 // single statment 
			 if(sqlparser.sqlstatements.size()==1){
				 TCustomSqlStatement querystmt=sqlparser.sqlstatements.get(0);
				 switch(querystmt.sqlstatementtype){
				 case sstselect:
					 TSelectSqlStatement selectStmt=(TSelectSqlStatement) querystmt;
					 if(!singleTabelStmt(selectStmt)){
						 break;
					 }
					if(!hasEqualExpr(selectStmt)){
						break;
					}
					result=0;
					if(debug){
						System.out.printf("qualifying cmd:%s\n", cmd);
					}
					break;
				default:
					break;
					 
				 }
			 }
		 }
		 
		 
		 return result;
		
	}

	private static void runHiveCmdUpdate(Statement stmt,String query) throws Exception{
		stmt.execute(query);
	}
	
	// get meta data 
	private static void getMetadata() throws Exception{
		
		String queryFormat="show columns from %s"; 

		Connection conn = null;
			try{
				// better to read for configuration 
				conn= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
				Statement stmt = conn.createStatement();
				
				String query=String.format(queryFormat, tableName); 
				ResultSet rs = stmt.executeQuery(query);

				int count=0;


				while(rs.next()){
					meta.append(count,rs.getString(1));
					count+=1;
				}

				//System.out.printf("Debug: %s \n", meta.toString());


				//done 
			}
			finally {
	            if (conn != null) {
	                conn.close();
	            }
        }

	}

	public static String getResult(){
		return result;
	}





	public static void getCorrelationsMetaStore() throws Exception{
		String queryFormat="select * from shcorrelation where shc_tname=\'%s\'";

		Connection conn=null;

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
           	String query=String.format(queryFormat, tableName); 

         
           	ResultSet rs = st.executeQuery(query);

           	// System.out.printf("open metastore sucessful \n");
          	// Hard code is typical wrong choice 

           	while(rs.next()){
           		Correlation cor=new Correlation(rs);
           		correlations.add(cor);

           	}

        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }
        // done 
	}



	public static void run(){
		try{
			getMetadata(); // Get Meta information  
			getCorrelationsMetaStore(); // Get Correlation from MetaStore 
			extract();	// extract source val from input query 
			violationTest(); // test whether it belongs to violated tuples 
			getTargetVals(); // if not violated; get its targetVals 
	     	refactorCmd();  //  need get equal attribute 

		}catch(Exception e){
				System.err.println("Caught: " + e.getMessage());
		}

	}


	private static void violationTest(Correlation cor) throws Exception{

		String violateQueryFormat="select * from SHCViolation where shc_name=\'%s\' and shc_val=\'%s\'";

		Connection conn = null;
		try{
				// better to read for configuration 
				conn= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
				Statement stmt = conn.createStatement();

				String query=String.format(violateQueryFormat,cor.SHC_NAME, cor.SHC_SVal);
				// String query=String.format(queryFormat, tableName); 
				ResultSet rs = stmt.executeQuery(query);
				if(debug){
					System.out.printf("test violation %s \n", query); 
				}
				while(rs.next()){
					cor.setViolate();
					if(debug){
						System.out.printf("%s set violated \n", cor.SHC_NAME);
					}
					break;
				}
				//done 
			}
			finally {
	            if (conn != null) {
	                conn.close();
	            }
        }


	}

	private static void violationTest(){
		for(Correlation cor: correlations){
			if(cor.isHard()){
				continue; 
			}

			try{
				violationTest(cor); // done 
			}catch(Exception e){
				System.err.println("Caught: " + e.getMessage());
			}
		}
	}

	private static void scriptLoadding(String correlationName, Statement stmt){


		String homePath=System.getenv("HIVE_HOME");
		String scriptPath=homePath+"/userScripts/";
		String loadQueryFormat="add file %s%s%s";

		String loadquery=String.format(loadQueryFormat, scriptPath, correlationName, ".py");
		try{
			runHiveCmdUpdate(stmt,loadquery);
		}catch(Exception e){
			System.err.println("ScriptLoadding: " + e.getMessage());
		}
		if(log){
			System.out.printf("loadding script via %s \n", loadquery);
		}

	}

	//
	private static void getTargetVal(Correlation cor) throws Exception{
		if(log){
			System.out.print("get Target Val \n");
		}

		String targetValQueryFormat="select transform(%s) using \'%s %s.%s\'";

		Connection conn = null;
			try{
				// better to read for configuration 
				conn= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
				Statement stmt = conn.createStatement();

				scriptLoadding(cor.SHC_NAME, stmt); 


				String query=String.format(targetValQueryFormat, cor.SHC_SVal, "python", cor.SHC_NAME, "py");
				// String query=String.format(queryFormat, tableName); 
				ResultSet rs = stmt.executeQuery(query);
				if(debug){
					System.out.printf("get target val via %s \n", query); 
				}
				while(rs.next()){
					//System.out.printf("rs:%s \n", rs.getString(1));

					cor.setTragetVal(rs.getString(1));
				}

				

				
				//done 
			}
			finally {
	            if (conn != null) {
	                conn.close();
	            }
        }

	}

	private static void getTargetVals(){

		for(Correlation cor: correlations){
			// skip useless correlation 
			if(cor.isNullSval()){
				continue;
			}
			// skip violated correlation while no maintained 
			if(cor.SHC_VIOLATED && !cor.isMaintain()){
				continue;
			}
			try{
				getTargetVal(cor);
			}catch(Exception e){
				System.err.println("Correlation get target Val Caught:  " + e.getMessage());
			}
		}
		// done 
	}


	public static void extract(){
		 TCustomSqlStatement querystmt=sqlparser.sqlstatements.get(0);
		 TSelectSqlStatement selectStmt=(TSelectSqlStatement) querystmt;
		 TExpression expr = selectStmt.getWhereClause().getCondition();
		 ExprVisitorExtract extractor=new ExprVisitorExtract(meta,correlations);
	     expr.postOrderTraverse(extractor);
	     // done
	}

	/**
	 * 
	 * case 1: no correlation with this attribute--> original query  
	 * case 2: with correlation and no violated --> refactor with targetVals   
	 * case 3: with correlation and violated , no maintence --> origianl query 
	 * case 4: with correlation and violated , maintance --> refactor with union query with targetVals 
	 */
	public static void refactorCmd(){
		 TCustomSqlStatement querystmt=sqlparser.sqlstatements.get(0);
		 TSelectSqlStatement selectStmt=(TSelectSqlStatement) querystmt;
		 TExpression expr = selectStmt.getWhereClause().getCondition();
		
		
		 boolean refacorFlag=false;
		 boolean unionFlag=false;
		//
		if(correlations.size()!=0){
			for(Correlation cor: correlations){
				if(!cor.isNullTval()){

					if(!cor.SHC_VIOLATED){
						refacorFlag=true;
						break;
					}else{
						unionFlag=true;
						break;
					}
				}
			}
		}



	    
	    if(!unionFlag){
	    	// case 2
		    if(refacorFlag){
		     	ExprVisitorRefactor  refactor=new ExprVisitorRefactor(meta,correlations);
		     	expr.postOrderTraverse(refactor);
		   		

		 	}
		 	// case 1, 3
		 	result=selectStmt.toString();
		 }else{
		 	if(debug){
		 		System.out.printf("refactor cmd with union cmd  \n");
		 	}
		 	// refactor 
		 	ExprVisitorRefactor  refactor=new ExprVisitorRefactor(meta,correlations);
		    expr.postOrderTraverse(refactor); 

		    String orgCmd=sqlparser.sqltext;
		    String shcName="";
		    for(Correlation cor: correlations){
		    	if(!cor.isNullTval()){
					shcName=correlations.get(0).SHC_NAME;
					break;
				}
			}
			String vTableName=String.format("%s_%s", tableName, shcName);
		    String unionCmd=orgCmd.replace(tableName,vTableName);

		    System.out.printf("union command :%s \n", unionCmd);

		    result=String.format("%s UNION %s", selectStmt.toString(), unionCmd); 


		 	// result=selectStmt.toString();

		 }
	 	
	}
	
	
// 	public static void main(String[] args){
// 		//test 
		
// 		String cmd="select * from test where y=7 ";
// 		String errCmd="select * from test, test2";
		
// 		int result=parse(cmd);
// //		System.out.printf("Parse Result:%d \n",result);
// 		TableMetadata meta=new TableMetadata();
		
// 		meta.append(0, "x");
// 		meta.append(1, "y");
		
// 		Correlation cExample=new Correlation();
		
// 		ArrayList<Correlation> correlations =new ArrayList<Correlation>();
// 		correlations.add(cExample);
// 		if(result!=-1){
// 			System.out.printf("related table:%s \n",tableName);
// 			refactor(meta,correlations);
// 		}
// 	}
	

}
