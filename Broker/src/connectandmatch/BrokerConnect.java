/*
 * This is The main Broker program for the probabilistic record matching, To match two source tables based on 
 * their LSH signatures
 * 
 * By: Ibrahim Lazrig, Lazrig@cs.colostate.edu or Lazrig@gmail.com
 */


package connectandmatch;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;



import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import brokerconfig.Config;
import brokerconfig.SysConfigMainWindow;

public class BrokerConnect {
	// JDBC driver name and database URL
	//static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String JDBC_DRIVER="org.postgresql.Driver";
	

	//  Database credentials
	//static  String ResultsDbUSER = "user1";
	//static  String ResultsDbUserPASS = "User1";
	static  String Timesfilename= "TimingResults2.txt";
	static  String Fpfilename= "FpResults2.txt";
	static  String EN_Fpfilename= "EN_FpResults2.txt";
	static  PrintWriter Rout = null;
	static PrintWriter FpRout = null;
	static PrintWriter EN_FpRout = null; 
	// combine both extBiGrams and non ExtBiGrams 
	static boolean CombineBoth_EN=true;
	
	

	public static void main(String[] args) {
		BrokerConnect db1=new BrokerConnect();
		
		Config CurrentConf=new Config();
		
		SetConfiguration(CurrentConf);
		
		//boolean UseExtBiGrams=true,Lcase=false;
		//boolean newSession=false; 
		//boolean newSession=true;
		// newSession For doing the matching processes again, if not then just compute Fp,Fn,Tp
		//StartOver to create the SigTables again
		//boolean UsePhon=false;
		long endTime ;
		long duration;
		long startTime;
		Calendar cal=Calendar.getInstance();
		
		try {
			char ExtBi=(CurrentConf.UseExtBiGrams)? 'E':'N';
			String t=""+cal.getTime();
			String FolderName="Results/Test_"+t;
		    File folder=new File(FolderName);
		    folder.mkdirs();
		    
			Timesfilename=FolderName+"/"+ ExtBi+"_Timing"+".txt";
			 Fpfilename= FolderName+"/"+ExtBi+"_FpResults"+".txt";
			 EN_Fpfilename= FolderName+"/"+"EN"+"_FpResults"+".txt";
			 
		    Rout = new PrintWriter(new BufferedWriter(new FileWriter(Timesfilename, true)));
		    Rout.println("Broker Begin Timing:"+cal.getTime());
		    
		    FpRout = new PrintWriter(new BufferedWriter(new FileWriter(Fpfilename, true)));
		    FpRout.println("\nBroker Begin New Test:"+cal.getTime());
		
		    EN_FpRout = new PrintWriter(new BufferedWriter(new FileWriter(EN_Fpfilename, true)));
		    EN_FpRout.println("\nBroker Begin New Test Using Combined Ext & nonExt Bigrams	1q:"+cal.getTime());
		
		    
	
		
	//2)   ...  start matching
		startTime = System.currentTimeMillis();
		Rout.println("Matching MyTables for ExtBig="+CurrentConf.UseExtBiGrams +"  Started @:"+startTime);
		Rout.println("\n---------------------------------------------------------------------------------");
		//db1.MatchMyTablesScenarios(UseExtBiGrams,Lcase,newSession,UsePhon);
		
		
		db1.MatchMyTablesOnAllColsScenarios(CurrentConf);// UseExtBiGrams,Lcase,newSession);
		
		endTime = System.currentTimeMillis();
		duration = (endTime - startTime);
		Rout.println("\n---------------------------------------------------------------------------------");
		Rout.println("\n Ended @:"+endTime +" And lasted for : "+duration);
		System.out.println("\n This took : "+duration);
		
		
		
		}catch (IOException e) {
		    System.err.println(e);
		}finally{
		    if(Rout != null)
		        Rout.close();
		    if(FpRout != null)
		        FpRout.close();
		    if(EN_FpRout != null)
		        EN_FpRout.close();
		    
		    
		} 	
		
	}


//=============================================================================================
	public static  void SetConfiguration( Config CurrentConfig) {
		
		try {
			SysConfigMainWindow frame = new SysConfigMainWindow(CurrentConfig);
			frame.addWindowListener( new java.awt.event.WindowAdapter() {
				public void windowClosed( java.awt.event.WindowEvent e ) {
				synchronized( e.getSource() ) {
				e.getSource().notifyAll();
				}
				}
				} );
			
			frame.setVisible(true);
			
			synchronized( frame ) {
				System.out.println( "Waiting for frame to close ..." );
				frame.wait();
				}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	

}

	//=========================================================================	
	
public int calnGItems( int k, double s){
//r-log(r/k)/log(s) =0
	double r,min=k*1.0;
	int imin=0;
	for (int i=1;i<k;i++){
		r=Math.abs(i-Math.log((double)i/k)/Math.log(s));
		//System.out.println("r="+r);
		if (r<min) {
			min=r;
			imin=i;
		}
	}
	//System.out.println("min r="+min+"  imin="+imin);
return (imin);	
}	
	//=========================================================================	
	
	public void MatchMyTablesOnAllColsScenarios(Config CurrentConf){
	
		int[][] TpFpFnTMSigResults=new int[1][4] ;
		int[][] EN_TpFpFnTMSigResults=new int[1][4] ;
		
		Connection conn1 = null;//,conn2 = null, ResultConn=null;
		 //boolean DoPhase1=false;
		 boolean DoPhase1=CurrentConf.DoPhase1;
		// boolean newSession=CurrentConf.newSession;
		 boolean newSession=false;
		String[]  ColumnsUsed4Match;
		int minColsMatch=2;
		
		
		String Source1DBurl = CurrentConf.Source1DBurl;
		//String Source2DBurl = CurrentConf.Source2DBurl;
		
		String InTablesSource1 = CurrentConf.InTablesSource1;
		String InTablesSource2 = CurrentConf.InTablesSource2 ;
		String Source1User = CurrentConf.Source1User ;
		String Src1UserPass = CurrentConf.Src1UserPass;
		//String Source2User = CurrentConf.Source2User ;
		//String Src2UserPass = CurrentConf.Src2UserPass;
		 
		String[] TbleNames={InTablesSource1,InTablesSource2};
		
		String[] LinkageColumnsToMakeSigs =CurrentConf.LinkageColumnsToMakeSigs;
		String[] LinkColsOfSigsToRecs = CurrentConf.LinkColsOfSigsToRecs;
		// for broker ...String[] ColumnsUsed4CalculatingMatchTR = CurrentConf.ColumnsUsed4CalculatingMatchTR;
		int nHashes = CurrentConf.nHashes;
		
		//double SigSimilarityTreshold = CurrentConf.SigSimilarityTreshold;
		
		//for broker ... double minTotColsWeight4Match = CurrentConf.minTotColsWeight4Match;
		
		
		
		 
		boolean UseExtBiGrams=CurrentConf.UseExtBiGrams;
		//boolean UsePhon=CurrentConf.UsePhon;
		
		
		//int nGroups,nGItems= calnGItems(nHashes,SigSimilarityTreshold);
		
		//nGroups=nHashes/nGItems;
		
		
		int EachColnGroups[]=new int[CurrentConf.SimTRforEachColumn.length];
		int EachColnGItems[]=new int[CurrentConf.SimTRforEachColumn.length];
		for(int i=0;i<CurrentConf.SimTRforEachColumn.length;i++){
				EachColnGItems[i]= calnGItems(nHashes,CurrentConf.SimTRforEachColumn[i]);
				EachColnGroups[i]=nHashes/EachColnGItems[i];
		}
		
		
		System.out.println("\n nGroups=" +Arrays.toString(EachColnGroups));
		System.out.println("\n nGitems=" +Arrays.toString(EachColnGItems));
		
				
		
		try{
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);

			// Open a connection
			System.out.println("Connecting to database ...");
			conn1 = DriverManager.getConnection(Source1DBurl, Source1User, Src1UserPass);
			/*
			System.out.println("Connecting to database 2...");
			conn2 = DriverManager.getConnection(Source2DBurl, Source2User, Src2UserPass);
			
			System.out.println("Connecting to Results database ...");
			ResultConn = DriverManager.getConnection(ResultDB_URL, ResultsDbUSER, ResultsDbUserPASS);
			*/
			// Match Sigs.
			//Scenario generation
			if (newSession){
			
			//	"\n"+Arrays.toString(LinkageColumnsToMakeSigs)+" With Sim. TRs( "+Arrays.toString(CurrentConf.SimTRforEachColumn)+")\n Sig Tables for Table "+InTable+"_"+" BiGrams="+UseExtBiGrams+", Created Successfully ...");
				System.out.println("\nComputing Match for setting  ExtBiGrams="+UseExtBiGrams);
				Rout.println("\nComputing Match for Sig setting "+Arrays.toString(EachColnGroups)+"_"+Arrays.toString(EachColnGItems)+"\n ExtBiGrams="+UseExtBiGrams);
				long startTime = System.currentTimeMillis();
				
				MatchMyTablesOnAllCols(  conn1, TbleNames[0],  TbleNames[1], LinkageColumnsToMakeSigs,
						EachColnGroups,EachColnGItems,  UseExtBiGrams, false, DoPhase1) ;
			
				
				long endTime = System.currentTimeMillis();
				long duration = (endTime - startTime);
				System.out.println("\nComputing Match  completed "+" With DoPhase1= "+DoPhase1 + ", It took :"+duration);
				Rout.println("\nComputing Match for Sig  With DoPhase1= "+DoPhase1 +" took : "+duration);
				
				
				
			} // if newsession
			
			
			System.out.println("\nComputing TP,FP,FN for Match All Cols with setting "+Arrays.toString(EachColnGroups)+"_"+Arrays.toString(EachColnGItems)+" ExtBiGrams="+UseExtBiGrams);
			FpRout.println("\nComputing TP,FP,FN for Match All Cols with setting "+Arrays.toString(EachColnGroups)+"_"+Arrays.toString(EachColnGItems)+" ExtBiGrams="+UseExtBiGrams);
			FpRout.format("%n%30s\t%10s\t%10s\t%10s\t%15s","ColumnsUsed4Match","Tp","Fp","Fn","TrueMatches");
			FpRout.println("\n-------------------------------------------------------------------------");
			
			EN_FpRout.println("\nComputing TP,FP,FN for Match All Cols with setting "+Arrays.toString(EachColnGroups)+"_"+Arrays.toString(EachColnGItems)+" For Both ExtBigrams=true and false");
			EN_FpRout.format("%n%30s\t%10s\t%10s\t%10s\t%15s","ColumnsUsed4Match","Tp","Fp","Fn","TrueMatches");
			EN_FpRout.println("\n-------------------------------------------------------------------------");
			
			
			
			int maxCols=(int) (Math.pow(2,LinkageColumnsToMakeSigs.length) -1);
			for(int b=1;b<=maxCols;b++){
				String bs=Integer.toBinaryString(b);
				String sb=StringUtils.repeat("0", LinkageColumnsToMakeSigs.length-bs.length())+bs;//new StringBuffer(bs).reverse().toString();
				int nCols=StringUtils.countMatches(bs, "1");
				if (nCols>1){
					
				
					
				ColumnsUsed4Match=new String[nCols];
				int loc=-1;
				for(int c=0;c<nCols;c++){
					 loc=sb.indexOf('1', loc+1);
				
					 ColumnsUsed4Match[c]=LinkageColumnsToMakeSigs[loc];
				} //for c
				
				System.out.println("\n\n\n Begin New Scenario: ColumnsUsed4Match="+Arrays.toString(ColumnsUsed4Match));
				//FpRout.println("\n\n\n Begin New Scenario:ColumnsUsed4Match="+Arrays.toString(ColumnsUsed4Match));
				
			//for(minColsMatch=2;minColsMatch<=ColumnsUsed4Match.length;minColsMatch++)
			//{
				//System.out.println("\n Number of colums should match are at least "+minColsMatch +" Out of the selected "+ColumnsUsed4Match.length);
				//FpRout.println("\n Case "+(minColsMatch-1)+":Number of colums should match are at least "+minColsMatch +" Out of the selected "+ColumnsUsed4Match.length);
				
				
					
					TpFpFnTMSigResults[0]=ComputeTpFpFn4MatchAllCols(conn1, TbleNames[0], TbleNames[1],LinkageColumnsToMakeSigs,LinkColsOfSigsToRecs,EachColnGroups,EachColnGItems,UseExtBiGrams,false,ColumnsUsed4Match,minColsMatch);
					if(CombineBoth_EN)
					EN_TpFpFnTMSigResults[0]=ComputeTpFpFn4MatchAllColsExt_NonExtBiGrams(conn1, TbleNames[0], TbleNames[1],LinkageColumnsToMakeSigs,LinkColsOfSigsToRecs,EachColnGroups,EachColnGItems,ColumnsUsed4Match,minColsMatch);
			
			// write results to file
						//FpRout.println("\n TP,FP,FN for MatchAllCols with setting "+" ExtBiGrams="+UseExtBiGrams);
						//FpRout.println("\n Sig.Settings"+Arrays.toString(EachColnGroups));
						//FpRout.format("%n%10s%10s%10s%15s","Tp","Fp","Fn","TrueMatches");
						//FpRout.println("\n-------------------------------------------------------------------------");
					FpRout.format("%n%30s",Arrays.toString(ColumnsUsed4Match));
							//FpRout.format("%7d-%-7d",nGroups,nGItems);
							for(int j=0;j<4;j++)
								FpRout.format("\t%10d",TpFpFnTMSigResults[0][j]);
							FpRout.println("");
							
					EN_FpRout.format("%n%30s",Arrays.toString(ColumnsUsed4Match));
							//FpRout.format("%7d-%-7d",nGroups,nGItems);
							for(int j=0;j<4;j++)
								EN_FpRout.format("\t%10d",EN_TpFpFnTMSigResults[0][j]);
							EN_FpRout.println("");
							
						
				// print to console
						//System.out.println("\n TP,FP,FN for MatchAllCols with setting "+" ExtBiGrams="+UseExtBiGrams);
						//System.out.println("\n Sig.Settings"+Arrays.toString(EachColnGroups));
						System.out.format("%n%10s%10s%10s%15s","Tp","Fp","Fn","TrueMatches");
						System.out.println("\n-------------------------------------------------------------------------");
						
							//System.out.format("%7d-%-7d",nGroups,nGItems);
							for(int j=0;j<4;j++)
							     System.out.format("%10d",TpFpFnTMSigResults[0][j]);
							System.out.println("");
							
						
			//}//for minColsMatch		
			}// if nCols
			} // for b
		
			
			
			
			
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
			//ExitNicely()
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
			//ExitNicely()
		}finally{
			//finally block used to close resources

			try{
				if(conn1!=null)
					conn1.close();
				/*if(conn2!=null)
					conn2.close();
				if(ResultConn!=null)
					ResultConn.close();
				*/
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
		System.out.println("End of matching AllCols !");
				
	}
//==================================================================================================================	
	
/* Exp. We need to define the set of columns to use in order to declare a match
 e.g {firstname,lastname,DateOB} or {lastname,DateOB,Cob,Sex} or ..
 then compare Fn,Fp of each set.
 1) decide on which columns goes for phase 1 (initial match selection)
    and which goes for phase 2 (filtering Fp).
    e.g ph1={firstname,lastname} and ph2={DateOB}
    in my code i assumed first 2 columns in the list are for ph1 and the ph2 columns are 
    given in a separate list
    
    So I'm going to create scenarios for all possible combination:
    I'll select one attribute(column) as main, add to it another one and consider the
    remaining attributes as aux(extra) .
    start with firstname, iteratively add to it one of the other attributes as:
    {firstname,lastname},{dob,cob}
    {firstname, dob}, {lastname,cob}
    {firstname,cob},{lastname,dob}
    {lastname,dob},{firstname,cob}
    {lastname,cob},{firstname,dob}
    {dob,cob},{firstname,lastname}
    we can also use with the main columns one or more aux columns, e.g 
    1-{firstname,lastname},{dob}
    2-{firstname,lastname},{cob}
    3-{firstname,lastname},{dob,cob}
    we get up to 6x3=18 different scenarios
    */
	
	public void MatchMyTablesScenarios(boolean UseExtBiGrams,boolean Lcase, boolean newSession,boolean UsePhon){
		String[] SigCNames={"firstname","lastname","cob","DateOB"} ; //{"DateOB"};// {"cob"};//{"firstname","lastname"};
		//String[] ExtCols={"cob","DateOB"};
		String[] TbleNames={"Source2A","Source2B"};
		//String[] UseExtCols;
		String[] MainCols=new String[2];
		String[] AuxCols=new String[SigCNames.length-2];
		String[] SelctedAuxCols;
		String[] MainAndAuxCols;
		//Scenario generation
		
		for(int i=0;i<SigCNames.length-1;i++){
			for(int j=i+1;j<SigCNames.length;j++){
				int t=0;
				MainCols[0]=SigCNames[i];
				MainCols[1]=SigCNames[j];
				
				for(int k=0;k<SigCNames.length;k++){
					if(k!=i && k!=j) AuxCols[t++]=SigCNames[k];
				}
				//to generate all possible combinations of AuxCols and call MatchMytables with each
				int maxCols=(int) (Math.pow(2,AuxCols.length) -1);
				for(int b=1;b<=maxCols;b++){
					String bs=Integer.toBinaryString(b);
					String sb=StringUtils.repeat("0", AuxCols.length-bs.length())+bs;//new StringBuffer(bs).reverse().toString();
					int nCols=StringUtils.countMatches(bs, "1");
					SelctedAuxCols=new String[nCols];
					int loc=-1;
					for(int c=0;c<nCols;c++){
						 loc=sb.indexOf('1', loc+1);
					
						SelctedAuxCols[c]=AuxCols[loc];
					} //for c
					MainAndAuxCols=(String[]) ArrayUtils.addAll(MainCols,AuxCols);
					System.out.println("\nBegin New Scenario: MainCols="+Arrays.toString(MainCols)+" SelectedAuxCols="+Arrays.toString(SelctedAuxCols));
					FpRout.println("\nBegin New Scenario: MainCols="+Arrays.toString(MainCols)+" SelectedAuxCols="+Arrays.toString(SelctedAuxCols));
					MatchMyTables(UseExtBiGrams,Lcase,newSession,UsePhon, TbleNames,MainAndAuxCols, AuxCols, SelctedAuxCols);
				} // for b
				
			}//j
		}// i
		
		
	}
//==================================================================================================================
//This function to create a Matching Table that contains the union of all matched Id's based on each single attribute
// So each attribute has a corresponding M_Att column with int value  to represent the number of matched Sig. for that attribute	

//==================================================================================================================	
/*
 
-- I chose to insert only those records with at lest 2 column matches

insert into AllColsMatchResults (ARecid,BRecid,M_fn,M_ln,M_COB,M_DOB)
select * from (
select ARecid,BRecid,sum(M_fn) M_fn,sum(M_ln) M_ln,sum( M_COB) M_COB,sum(M_DOB) M_DOB from (
                       (select ARecid,BRecid ,1 as M_fn,0 as M_ln,0 as M_COB,0 as M_DOB  from NoNullSig16_3_E_firstnameMatchResults  ORDER BY ARecid)
			union all  (select  ARecid,BRecid ,0 as M_fn,1 as M_ln,0 as M_COB,0 as M_DOB  from NoNullSig16_3_E_lastnameMatchResults  ORDER BY ARecid)
			union all ( select  ARecid,BRecid ,0 as M_fn,0 as M_ln,0 as M_COB,1 as M_DOB  from NoNullSig16_3_E_DateOBMatchResults ORDER BY ARecid)
			union all  (select  ARecid,BRecid ,0 as M_fn,0 as M_ln,1 as M_COB,0 as M_DOB  from NoNullSig16_3_E_cobMatchResults  ORDER BY ARecid)
			 ) t group by ARecid,BRecid  
 ) tt where M_fn+ M_ln+M_COB+M_DOB >1 ;
 
 
 -- Need new Function to compute Fp,Tp,Fn see those matched with more than 2 columns
 select * from NoNullSig16_3_E_AllColsMatchResults where  M_fn+ M_ln+M_COB+M_DOB >1 and ARecid!=BRecid;


 * 	
 */
//==================================================================================================================
	public int MatchMyTablesOnAllCols( Connection conn1,String Tble1, String Tble2, String[] SigCNames,
			int[] nGroups,int[] nGItems,boolean UseExtBiGrams,boolean Lcase, boolean DoPhase1) {
	
	
	int numberOfMatchedSig=1;
	Statement stmt = null;
	String sqlCreateTbl,sqlDrop;
	String InsertSQL;
	String SigSettings="";
	
	String[] UnionColNames=new String[2+SigCNames.length]; 
	UnionColNames[0]="ARecid";
	UnionColNames[1]="BRecid";
	String[] UnionSelCols=new String[SigCNames.length];
	String ResultSelCols="ARecid,BRecid";
	String WhereCond="";
	String InsertSelCols="ARecid,BRecid";
	String TbleMatchCols="";
	
	
	//select ARecid,BRecid,sum(M_fn) M_fn,sum(M_ln) M_ln,sum( M_COB) M_COB,sum(M_DOB) M_DOB
	for(int i=0;i<SigCNames.length;i++){
		SigSettings=SigSettings+nGroups[i]+"_"+nGItems[i]+"_";
		UnionColNames[i+2]="M_"+SigCNames[i];
		ResultSelCols=ResultSelCols+","+"SUM("+UnionColNames[i+2]+") "+UnionColNames[i+2];
		
		InsertSelCols=InsertSelCols+","+UnionColNames[i+2];
		//to select only those records with at lest 2 column matches
		//where M_fn+ M_ln+M_COB+M_DOB >1 
		// Changed for postgresql to  M_fn >= 1 and M_ln>=1 and M_COB>=1 and M_DOB >=1, where 1 is number of mached sig.s 
		// to find if at least each 2 columns where matched (i.e each column has sig match >numberOfMatchedSig) we used "if" to test each column (0 not matched and 1 matched) 
		
		
		// If works only in Mysql
		/*if(i==0) WhereCond=" IF("+UnionColNames[i+2]+">="+numberOfMatchedSig+",1,0) ";
		else
			WhereCond=WhereCond+" + "+" IF("+UnionColNames[i+2]+">="+numberOfMatchedSig+",1,0) ";
		*/
		if(i==0) WhereCond=" (CASE When "+UnionColNames[i+2]+">="+numberOfMatchedSig+" Then 1 Else 0 END ) ";
		else
			WhereCond=WhereCond+ " + " + " (CASE When "+UnionColNames[i+2]+">="+numberOfMatchedSig+" Then 1 Else 0 END ) ";

		//`M_fn`  boolean DEFAULT 0,
		if(i==0) TbleMatchCols=UnionColNames[i+2]+"  int DEFAULT 0 ";
		else
			TbleMatchCols=TbleMatchCols+","+UnionColNames[i+2]+" int DEFAULT 0";
	}
	
	WhereCond=WhereCond+">=2"; // at least matched by 2 cols
	
	//union all  (select  ARecid,BRecid ,0 as M_fn,1 as M_ln,0 as M_COB,0 as M_DOB  
	for(int i=0;i<SigCNames.length;i++){
		UnionSelCols[i]=UnionColNames[0]+","+UnionColNames[1];
		for(int j=0;j<SigCNames.length;j++)
			UnionSelCols[i]=(i==j)? UnionSelCols[i]+","+" 1 as "+UnionColNames[j+2] :
				                    UnionSelCols[i]+","+" 0 as "+UnionColNames[j+2];
	}
	
	String UnionResultSql="";
	
	char ExtBi=(UseExtBiGrams)? 'E':'N';
	
	String[] ph1ResultTbles=new String[SigCNames.length]; 
	
	//String ph2ResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"AllColsMatchResults":
	//	"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"AllColsMatchResults";
	
	String ph2ResultTble=(Lcase)? "LNoNullSig"+SigSettings+ExtBi+"_"+"AllColsMatchResults":
		"NoNullSig"+SigSettings+ExtBi+"_"+"AllColsMatchResults";
	
	String[] SigsOfCNameOfTble1=new String[SigCNames.length];
	String[] SigsOfCNameOfTble2=new String[SigCNames.length];
	
	
	for(int i=0;i<SigCNames.length;i++){
		SigsOfCNameOfTble1[i]=(Lcase)? "LNoNullSig":"NoNullSig";
		SigsOfCNameOfTble1[i]=SigsOfCNameOfTble1[i]+nGroups[i]+"_"+nGItems[i]+"_"+Tble1+ExtBi+SigCNames[i];
		
		SigsOfCNameOfTble2[i]=(Lcase)? "LNoNullSig":"NoNullSig"; 
		SigsOfCNameOfTble2[i]=SigsOfCNameOfTble2[i]+nGroups[i]+"_"+nGItems[i]+"_"+Tble2+ExtBi+SigCNames[i];
		
		ph1ResultTbles[i]=(Lcase)? "LNoNullSig"+nGroups[i]+"_"+nGItems[i]+"_"+ExtBi+"_"+SigCNames[i]+"MatchResults":
			               "NoNullSig"+nGroups[i]+"_"+nGItems[i]+"_"+ExtBi+"_"+SigCNames[i]+"MatchResults" ;
		
		//union all  (select  ARecid,BRecid ,0 as M_fn,1 as M_ln,0 as M_COB,0 as M_DOB 
		//               from NoNullSig16_3_E_lastnameMatchResults  ORDER BY ARecid)
		if(i==0) 
			UnionResultSql=" (select "+ UnionSelCols[i] +" from " +  ph1ResultTbles[i] +" ORDER BY ARecid)";
		else
			UnionResultSql=UnionResultSql+" union all  (select "+ UnionSelCols[i] +" from " +  ph1ResultTbles[i] +" ORDER BY ARecid)";
	}
	
	//select * from (
	//select ARecid,BRecid,sum(M_fn) M_fn,sum(M_ln) M_ln,sum( M_COB) M_COB,sum(M_DOB) M_DOB from (....
	//  ...... ) tt where M_fn+ M_ln+M_COB+M_DOB >1 ;
	String USQL=" select * from ( select "+ ResultSelCols+" from ("+UnionResultSql+")  t group by ARecid,BRecid )tt where "+WhereCond;
	
	
	//insert into NoNullSig16_3_E_AllColsMatchResults (ARecid,BRecid,M_fn,M_ln,M_COB,M_DOB)
	InsertSQL=" insert into " +ph2ResultTble+" (" + InsertSelCols+") "+USQL+";";
	
	//drop table if exists NoNullSig16_3_E_AllColsMatchResults;
	sqlDrop="Drop Table If exists "+ph2ResultTble+";";
	
	
	sqlCreateTbl = "CREATE TABLE if not exists "+ph2ResultTble+" ("+
			
			"ARecid int DEFAULT NULL,"+
			"BRecid int DEFAULT NULL,"+
			TbleMatchCols+
			
			") ;";
	
	
	String[] t1selectCols, t2selectCols,T1SelAliaces, T2SelAliaces,T1T2CondCols,CondOps,CondJunc;
	
	  // phase 1 means first stage of matching, i.e. matching firstnames
	 t1selectCols=new String[] {"RecId"};    //[0].equals("RecId");
	 t2selectCols=new String[] {"RecId"};     //   [0].equals("RecId");
	 T1SelAliaces=new String[]{"ARecId"};
	 T2SelAliaces=new String[]{"BRecId"};
	 T1T2CondCols=new String[]{"Sig","SigId"};
	 CondOps =new String[]{"=","="};
	 CondJunc=new String[]{"and"};

	//phase 1
	 long startTime = System.currentTimeMillis();
	 System.out.println("\nComputing Match phase 1, nGroups" +Arrays.toString(nGroups)+"_nGItems"+Arrays.toString(nGItems)+ " started.. at "+startTime); 
	 Rout.println("\nComputing Match phase 1 started.. at "+startTime); 
	 Rout.format("%n%n%20s%20s%20s","Column","Sig. Settings","Duration"); 
	 Rout.println("\n--------------------------------------------------------------------------------");
	 
	 long endTime = System.currentTimeMillis();
	 long duration = (endTime - startTime);	
		
	 
	int res=(DoPhase1)? 0:1;
	if(DoPhase1){
	for(int i=0;i<SigCNames.length;i++){	
		startTime = System.currentTimeMillis();
		System.out.println("\n.....Match phase 1, for Column "+SigCNames[i]+" With Sig. Setting "+nGroups[i]+"_"+nGItems[i] +" started.. at "+startTime); 
		//Rout.println("\n.....Match phase 1, for Column "+SigCNames[i]+ " With Sig. Setting "+nGroups[i]+"_"+nGItems[i] +" started.. at "+startTime);
		
			
	  res=Join2TablesByNameOnCondition( conn1,SigsOfCNameOfTble1[i], SigsOfCNameOfTble2[i],t1selectCols,t2selectCols,
			T1SelAliaces ,T2SelAliaces,ph1ResultTbles[i], T1T2CondCols, CondOps , CondJunc);
	  
	  endTime = System.currentTimeMillis();
	  duration = (endTime - startTime);
	  System.out.println("\n.....Match phase 1, for Column "+SigCNames[i]+" With Sig. Setting "+nGroups[i]+"_"+nGItems[i] + " Completed. It took "+duration); 
		// Rout.println("\n.....Match phase 1, for Column "+SigCNames[i]+ " With Sig. Setting "+nGroups[i]+"_"+nGItems[i] + " Completed. It took "+duration); 
	  Rout.format("%n%20s %9d-%-9d%20d",SigCNames[i],nGroups[i],nGItems[i],duration);  
	}
	} //DoPhase1
	
	
	 Rout.println("\n--------------------------------------------------------------------------------");
	//phase 2
	startTime = System.currentTimeMillis();
	System.out.println("\nComputing Match phase 2, " +Arrays.toString(nGroups)+"_nGItems"+Arrays.toString(nGItems)+  " started.. at "+startTime); 
	 Rout.println("\nComputing Match phase 2 started.. at "+startTime); 
		
	if(res==1)
	{
	try{
		
			
		stmt = conn1.createStatement();
		System.out.println("\n Deleting old Match phase2 result table ("+ph2ResultTble+ ") if exist ... ");
		stmt.executeUpdate(sqlDrop);
		System.out.println(" Ok !");
		System.out.println("\n Creating new Match phase2 result table("+ph2ResultTble+ ")  ... ");
		stmt.executeUpdate(sqlCreateTbl);

		String sqlDropndx,sqlAddndx;
		String[] Indexes={"ARecid","BRecid","ARecid,BRecid"};
		for(int ndx=0;ndx<Indexes.length;ndx++){
			sqlDropndx="DROP INDEX  if exists "+ph2ResultTble+"ndx"+ndx;
			sqlAddndx="CREATE INDEX "+ph2ResultTble+"ndx"+ndx+
						  " ON " +ph2ResultTble+" USING btree ("+Indexes[ndx]+"); ";
			stmt.executeUpdate(sqlDropndx);
			stmt.executeUpdate(sqlAddndx);
		}
		
		System.out.println(" Ok !");
		System.out.println("\n Finding Matches, and filling phase2 Match table  Please wait ...");
		
		startTime = System.currentTimeMillis();
		
		stmt.executeUpdate(InsertSQL);
		
		 endTime = System.currentTimeMillis();
		  duration = (endTime - startTime);
		  System.out.println("\n.....Match phase 2,  Completed. It took "+duration); 
			 Rout.println("\n.....Match phase 2 Completed. It took "+duration); 
				 
		//System.out.println(" Completed !\n"); 

		stmt.close();
	}catch(SQLException se){
		//Handle errors for JDBC
		se.printStackTrace();
		
		//ExitNicely()
	}catch(Exception e){
		//Handle errors for Class.forName
		e.printStackTrace();
		//ExitNicely()
	
		return 0;
	}//end try
	}// if res==1
	System.out.println("End of MatchMyTablesOnAllCols!");
	return 1;
}//end MatchMyTablesOnAllCols
//=================================================================================================================
//	-- Need new Function to compute Fp,Tp,Fn see those matched with more than 2 columns
//	 select * from AllColsMatchResults where  M_fn+ M_ln+M_COB+M_DOB >1 and ARecid!=BRecid;

//=========================================================================	
		// MatchAllCols version of ComputeTpFpFn
		// OrgLink is the array of fields used to link the records
		public int[] ComputeTpFpFn4MatchAllCols(Connection conn,String Tble1,String Tble2, String[] SigCNames,String[] OrgLink,
				int[] nGroups,int[] nGItems,boolean UseExtBiGrams,boolean Lcase,String[]  ColumnsUsed4Match,int MatchTR) {
			int[] result=new int[4];
			int Tp=0,Fp=0,Fn=0,tot=0;
			
			// the minimum number of matched sig for the column to say match
			int numberOfMatchedSigForCols = 1; // TODO: we should define one for each column
			
			Statement stmt = null;
			String Cond="";
			String TPsql,FPsql,FNsql,TotOrgMatchsql;
			//MatchTR is used to signify how many of the ColumnsUsed4Match should be satisfied
			// e.g. any 2 of 3 or all 3 of 3, so the condition will be sum of ColumnsUsed4Match>=MatchTR
			// that is, at least MatchTR of the colums matched
			for(int i=0;i< ColumnsUsed4Match.length;i++){
				if(i==0) Cond="M_"+ColumnsUsed4Match[i]+">=" +numberOfMatchedSigForCols;
				else
				   Cond=Cond +" and  "+"M_"+ColumnsUsed4Match[i]+">=" +numberOfMatchedSigForCols ;
			}
			// Cond=Cond+" >= "+MatchTR;
			
			char ExtBi=(UseExtBiGrams)? 'E':'N';
			
			String SigSettings="";
			
			for(int i=0;i<SigCNames.length;i++)
			        SigSettings=SigSettings+nGroups[i]+"_"+nGItems[i]+"_";
			//String ph2ResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"AllColsMatchResults":
			//	"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"AllColsMatchResults";
			String ph2ResultTble=(Lcase)? "LNoNullSig"+SigSettings+ExtBi+"_"+"AllColsMatchResults":
				"NoNullSig"+SigSettings+ExtBi+"_"+"AllColsMatchResults";
			//String MatchResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults":
			//	"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults" ;
			
			FPsql="select count(*) as fp from "+ ph2ResultTble+"  r where r.ARecid!=r.BRecid"+" and "+Cond+";";
			TPsql="select count(*) as tp from "+ ph2ResultTble+"  r where r.ARecid=r.BRecid "+" and "+Cond+";";
			
			
			TotOrgMatchsql=" select   count(*) as OrgTrueMatch from ( " 
			         +"( "
			         +"  Select "+ OrgLink[0] +" ARecid from "+ Tble1+ " "
			         +") A" 
			            +"  join "
			         +"( " 
			         +  " Select "+OrgLink[0]+" BRecid from " + Tble2+ " "
			         +" ) B " 
			            +"  on"
			          +" ARecid=BRecid" 
			       +") ; ";
				
				FNsql="select count(*) as fn from "
			        +"("
						+"select opt.ARecid oa, opt.BRecid ob, r.ARecid ra, r.BRecid rb from " 
			      +"("
			      +" select   * from " 
			         +"( "
			         +"  Select "+OrgLink[0]+"  ARecid from "+ Tble1+ "  "
			         +") A" 
			            +"  join "
			         +"( " 
			         +  " Select "+OrgLink[0]+" BRecid from " + Tble2+ "  "
			         +" ) B " 
			            +"  on"
			          +" ARecid=BRecid" 
			       +") opt "  
			          
			        +" Left Join "
			        +"("
			        +" select * from "
			       + ph2ResultTble +"  Where "+Cond +") r "  
			          +" on "
			       +" r.ARecid= opt.ARecid and r.BRecid = opt.BRecid " 
			       
			      +" where r.ARecid is NULL  " 
				+") xx ;";

			try{
				stmt=conn.createStatement();
				ResultSet rs = stmt.executeQuery(FPsql);
				if (rs.next())
					Fp=rs.getInt("fp");
				
				rs = stmt.executeQuery(TPsql);
				if (rs.next())
					Tp=rs.getInt("tp");
				
				rs = stmt.executeQuery(FNsql);
				if (rs.next())
					Fn=rs.getInt("fn");
				
				rs = stmt.executeQuery(TotOrgMatchsql);
				if (rs.next())
					tot=rs.getInt("OrgTrueMatch");
				
				stmt.close();
			}catch(SQLException se){
				//Handle errors for JDBC
				se.printStackTrace();
				//ExitNicely()
			}catch(Exception e){
				//Handle errors for Class.forName
				e.printStackTrace();
				//ExitNicely()
			
				
			}
			
			result[0]=Tp;
			result[1]=Fp;
			result[2]=Fn;
			result[3]=tot;
			
			return result;
		}
			
	
	
//==================================================================================================================	

		public int[] ComputeTpFpFn4MatchAllColsExt_NonExtBiGrams(Connection conn,String Tble1,String Tble2, String[] SigCNames,String[] OrgLink,
				int[] nGroups,int[] nGItems,String[]  ColumnsUsed4Match,int MatchTR) {
			int[] result=new int[4];
			int Tp=0,Fp=0,Fn=0,tot=0;
			
			// the minimum number of matched sig for the column to say match
			int numberOfMatchedSigForCols = 1; // TODO: we should define one for each column
			
			Statement stmt = null;
			String Cond="";
			String TPsql,FPsql,FNsql,TotOrgMatchsql;
			//MatchTR is used to signify how many of the ColumnsUsed4Match should be satisfied
			// e.g. any 2 of 3 or all 3 of 3, so the condition will be sum of ColumnsUsed4Match>=MatchTR
			// that is, at least MatchTR of the colums matched
			for(int i=0;i< ColumnsUsed4Match.length;i++){
				if(i==0) Cond="M_"+ColumnsUsed4Match[i]+">=" +numberOfMatchedSigForCols;
				else
				   Cond=Cond +" and  "+"M_"+ColumnsUsed4Match[i]+">=" +numberOfMatchedSigForCols ;
			}
			// Cond=Cond+" >= "+MatchTR;
			
			
			String SigSettings="";
			
			for(int i=0;i<SigCNames.length;i++)
			        SigSettings=SigSettings+nGroups[i]+"_"+nGItems[i]+"_";
			//String ph2ResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"AllColsMatchResults":
			//	"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"AllColsMatchResults";
			String E_ph2ResultTble="NoNullSig"+SigSettings+"E"+"_"+"AllColsMatchResults";
			String N_ph2ResultTble="NoNullSig"+SigSettings+"N"+"_"+"AllColsMatchResults";
			String EN_ph2ResultTble="NoNullSig"+SigSettings+"N_E"+"_"+"AllColsMatchResults";
			//String MatchResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults":
			//	"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults" ;
		
			
			String TbleMatchCols="";
			
			
			
			for(int i=0;i<SigCNames.length;i++){
				if(i==0) TbleMatchCols= "M_"+SigCNames[i]+"  int DEFAULT 0 ";
				else
					TbleMatchCols=TbleMatchCols+","+"M_"+SigCNames[i]+" int DEFAULT 0";
			}
			
					
	String	sqlCreateENTbl = "CREATE TABLE if not exists "+EN_ph2ResultTble+" ("+				
			"ARecid int DEFAULT NULL,"+
			"BRecid int DEFAULT NULL,"+
			TbleMatchCols+
			") WITH (  OIDS=FALSE);";
			

	String sqlDropENtbl="Drop Table If exists "+EN_ph2ResultTble+";";
	
	String MergeSql="Insert into "+EN_ph2ResultTble+" select * from "+ E_ph2ResultTble+ 
					" union select * from "+ N_ph2ResultTble ;




			
			
			
			FPsql="select  count(*) as fp from (select distinct r.ARecid,r.BRecid  from "+ EN_ph2ResultTble+"  r where r.ARecid!=r.BRecid"+" and "+Cond+") t;";
			TPsql="select  count(*) as tp from (select distinct r.ARecid,r.BRecid  from "+ EN_ph2ResultTble+"  r where r.ARecid=r.BRecid "+" and "+Cond+") t;";
			
			
			TotOrgMatchsql=" select   count(*) as OrgTrueMatch from ( " 
			         +"( "
			         +"  Select "+ OrgLink[0] +" ARecid from "+ Tble1+ " "
			         +") A" 
			            +"  join "
			         +"( " 
			         +  " Select "+OrgLink[0]+" BRecid from " + Tble2+ " "
			         +" ) B " 
			            +"  on"
			          +" ARecid=BRecid" 
			       +") ; ";
				
				FNsql="select  count(*) as fn from "
			        +"("
						+" select distinct opt.ARecid oa, opt.BRecid ob, r.ARecid ra, r.BRecid rb from " 
			      +"("
			      +" select   * from " 
			         +"( "
			         +"  Select "+OrgLink[0]+"  ARecid from "+ Tble1+ "  "
			         +") A" 
			            +"  join "
			         +"( " 
			         +  " Select "+OrgLink[0]+" BRecid from " + Tble2+ "  "
			         +" ) B " 
			            +"  on"
			          +" ARecid=BRecid" 
			       +") opt "  
			          
			        +" Left Join "
			        +"("
			        +" select * from "
			       + EN_ph2ResultTble +"  Where "+Cond +") r "  
			          +" on "
			       +" r.ARecid= opt.ARecid and r.BRecid = opt.BRecid " 
			       
			      +" where r.ARecid is NULL  " 
				+") t ;";

			try{
				stmt=conn.createStatement();
				
				System.out.println("\n Deleting old AllCols Match E&N result table ("+EN_ph2ResultTble+ ") if exist ... ");
				stmt.executeUpdate(sqlDropENtbl);
				System.out.println(" Ok !");
				System.out.println("\n Creating new AllCols Match E&N result table("+EN_ph2ResultTble+ ")  ... ");
				stmt.executeUpdate(sqlCreateENTbl);

				
				String sqlDropndx,sqlAddndx;
				String[] Indexes={"ARecid","BRecid","ARecid,BRecid"};
				for(int ndx=0;ndx<Indexes.length;ndx++){
					sqlDropndx="DROP INDEX  if exists "+EN_ph2ResultTble+"ndx"+ndx;
					sqlAddndx="CREATE INDEX "+EN_ph2ResultTble+"ndx"+ndx+
								  " ON " +EN_ph2ResultTble+" USING btree ("+Indexes[ndx]+"); ";
					stmt.executeUpdate(sqlDropndx);
					stmt.executeUpdate(sqlAddndx);
				}
				
				System.out.println("\n Adding data to  new AllCols Match E&N result table("+EN_ph2ResultTble+ ")  ... ");
				stmt.executeUpdate(MergeSql);

				
				
				
				ResultSet rs = stmt.executeQuery(FPsql);
				if (rs.next())
					Fp=rs.getInt("fp");
				
				rs = stmt.executeQuery(TPsql);
				if (rs.next())
					Tp=rs.getInt("tp");
				
				rs = stmt.executeQuery(FNsql);
				if (rs.next())
					Fn=rs.getInt("fn");
				
				rs = stmt.executeQuery(TotOrgMatchsql);
				if (rs.next())
					tot=rs.getInt("OrgTrueMatch");
				
				stmt.close();
			}catch(SQLException se){
				//Handle errors for JDBC
				se.printStackTrace();
				//ExitNicely()
			}catch(Exception e){
				//Handle errors for Class.forName
				e.printStackTrace();
				//ExitNicely()
			
				
			}
			
			result[0]=Tp;
			result[1]=Fp;
			result[2]=Fn;
			result[3]=tot;
			
			return result;
		}
			
	
	
//==================================================================================================================	
		
		
		public void MatchMyTables(boolean UseExtBiGrams,boolean Lcase, boolean newSession,boolean UsePhon,
			String[] TbleNames,String[]SigCNames,String[] ExtCols,String[] UseExtCols){
		
		int[] nGroups={16,12,10,8,7,6,5},nGItems={3,4,5,6,7,8,10};
		
		//boolean UseExtBiGrams=true;
		Connection conn = null;
		int[][] TpFpFnTMSigResults=new int[nGroups.length][4] ;
		int[][] TpFpFnTMPhnResults=new int[3][4] ; //Dm,Sndx,Ny
		String[] Alg={"Sndx","DMPhone","NYIIS"};
		// just avoiding error
		String DB_URL="", USER="", PASS="";
		try{
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);

			// Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
		
			
			if(UsePhon){
			for(int i=0;i<3;i++){
				
				if (newSession){
					System.out.println("\nComputing Match for Phonetics Algorithm "+ Alg[i]);
					long startTime = System.currentTimeMillis();
					PhonMatch2Tables(conn, TbleNames[0],TbleNames[1], SigCNames,Alg[i]);
					long endTime = System.currentTimeMillis();
					long duration = (endTime - startTime);
					System.out.println("\nComputing Match  completed, It took :"+duration);
					Rout.println("\nComputing Match for Phonetics Algorithm "+ Alg[i]+" Took : "+duration);
				}
				System.out.println("\nComputing TP,FP,FN for Phonetics Algorithm "+ Alg[i]);
				TpFpFnTMPhnResults[i]=ComputePhonTpFpFn(conn, TbleNames[0], TbleNames[1],SigCNames,Alg[i]);
			}
			
			} //if UsePhon
			
			// Match Sigs.
			
			for(int i=0;i<nGroups.length;i++){
				if (newSession){
				System.out.println("\nComputing Match for setting "+nGroups[i]+"_"+nGItems[i]+" ExtBiGrams="+UseExtBiGrams+", Lcase="+Lcase);
				long startTime = System.currentTimeMillis();
				
				//Match2Tables(conn, TbleNames[0],TbleNames[1], SigCNames,nGroups[i], nGItems[i],UseExtBiGrams,Lcase);
				 MultiPhaseMatch2TablesOnSingleColumn(conn,TbleNames[0],TbleNames[1], SigCNames,nGroups[i], nGItems[i],UseExtBiGrams,Lcase,ExtCols);
				// add extra matching cols for result was added to the above matching function  
				//
				long endTime = System.currentTimeMillis();
				long duration = (endTime - startTime);
				System.out.println("\nComputing Match  completed, It took :"+duration);
				Rout.println("\nComputing Match for Sig "+nGroups[i]+"_"+nGItems[i]+" took : "+duration);
				} // if newsession
				
				
				System.out.println("\nComputing TP,FP,FN for setting "+nGroups[i]+"_"+nGItems[i]+" ExtBiGrams="+UseExtBiGrams+", Lcase="+Lcase);
				//TpFpFnTMSigResults[i]=ComputeTpFpFn(conn, TbleNames[0], TbleNames[1],SigCNames,nGroups[i],nGItems[i],UseExtBiGrams,Lcase);
				
				//String[] UseExtCols={"cob"};
				TpFpFnTMSigResults[i]=mphComputeTpFpFn(conn, TbleNames[0], TbleNames[1],SigCNames,nGroups[i],nGItems[i],UseExtBiGrams,Lcase,UseExtCols);
				
			}	
			
			// write results to file
			FpRout.println("\n TP,FP,FN for setting "+" ExtBiGrams="+UseExtBiGrams);
			FpRout.format("%n%15s%10s%10s%10s%15s","Sig.Settings","Tp","Fp","Fn","TrueMatches");
			FpRout.println("\n-------------------------------------------------------------------------");
			for(int i=0;i<nGroups.length;i++){
				FpRout.format("%7d-%-7d",nGroups[i],nGItems[i]);
				for(int j=0;j<4;j++)
					FpRout.format("%10d",TpFpFnTMSigResults[i][j]);
				FpRout.println("");
				
			}
			
			System.out.format("%n%15s%10s%10s%10s%15s","Sig.Settings","Tp","Fp","Fn","TrueMatches");
			System.out.println("\n-------------------------------------------------------------------------");
			for(int i=0;i<nGroups.length;i++){
				System.out.format("%7d-%-7d",nGroups[i],nGItems[i]);
				for(int j=0;j<4;j++)
				     System.out.format("%10d",TpFpFnTMSigResults[i][j]);
				System.out.println("");
				
			}
			if (UsePhon){
			System.out.println("\n-------------------------------------------------------------------------");
			for(int i=0;i<3;i++){
				System.out.format("%15s",Alg[i]);
				for(int j=0;j<4;j++)
				     System.out.format("%10d",TpFpFnTMPhnResults[i][j]);
				System.out.println("");
				
			}
			} // if UsePhon
			
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
			//ExitNicely()
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
			//ExitNicely()
		}finally{
			//finally block used to close resources

			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
		System.out.println("End of matching !");
			
	}
	
//=========================================================================	
	// multi-phase version of ComputeTpFpFn
	//  select count(*) from  Sig16_3_fn_ln_results r where r.ARecid!=r.BRecid  and r.`M_COB`=1; 
	public int[] mphComputeTpFpFn(Connection conn,String Tble1,String Tble2, String[] SigCNames,
			int nGroups,int nGItems,boolean UseExtBiGrams,boolean Lcase, String[] UseExtCols) {
		int[] result=new int[4];
		int Tp=0,Fp=0,Fn=0,tot=0;
		
		Statement stmt = null;
		String extCond="";
		String TPsql,FPsql,FNsql,TotOrgMatchsql;
		for(int i=0;i<UseExtCols.length;i++){
			extCond=extCond+" and "+"M_"+UseExtCols[i]+"=1 " ;
		}
		
		char ExtBi=(UseExtBiGrams)? 'E':'N';
		String ph2ResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"Ph2MatchResults"+SigCNames[0]+"_"+SigCNames[1]:
			"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"Ph2MatchResults" +SigCNames[0]+"_"+SigCNames[1];
		
		//String MatchResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults":
		//	"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults" ;
		
		FPsql="select count(*) as fp from "+ ph2ResultTble+"  r where r.ARecid!=r.BRecid"+extCond+";";
		TPsql="select count(*) as tp from "+ ph2ResultTble+"  r where r.ARecid=r.BRecid;";
		//TODO: for ext col, we might see those records declared not match based on the first 2 attributes, while they are matched by other extCols 
		/*TotOrgMatchsql=" select   count(*) as OrgTrueMatch from ( " 
	         +"( "
	         +"  Select A1.id ARecid from "+ Tble1+ " A1  where A1."+SigCNames[0]+" !=\"\" and A1."+SigCNames[1]+"!=\"\" "
	         +") A" 
	            +"  join "
	         +"( " 
	         +  " Select B1.id BRecid from " + Tble2+ " B1  where B1." + SigCNames[0]+" !=\"\" and B1."+SigCNames[1]+"!=\"\" "
	         +" ) B " 
	            +"  on"
	          +" ARecid=BRecid" 
	       +") ; ";
		
		FNsql="select count(*) as fn from "
	        +"("
				+"select opt.ARecid oa, opt.BRecid ob, r.ARecid ra, r.BRecid rb from " 
	      +"("
	      +" select   * from " 
	         +"( "
	         +"  Select A1.id ARecid from "+ Tble1+ " A1  where A1."+SigCNames[0]+" !=\"\" and A1."+SigCNames[1]+"!=\"\" "
	         +") A" 
	            +"  join "
	         +"( " 
	         +  " Select B1.id BRecid from " + Tble2+ " B1  where B1." + SigCNames[0]+" !=\"\" and B1."+SigCNames[1]+"!=\"\" "
	         +" ) B " 
	            +"  on"
	          +" ARecid=BRecid" 
	       +") opt "  
	          
	        +" Left Join "
	       + ph2ResultTble +" r "  
	          +" on "
	       +" concat(r.ARecid,r.BRecid) = concat(opt.ARecid,opt.BRecid) " 
	       
	      +" where r.ARecid is NULL  " 
		+") xx ;";*/
		
		
		TotOrgMatchsql=" select   count(*) as OrgTrueMatch from ( " 
		         +"( "
		         +"  Select id ARecid from "+ Tble1+ " "
		         +") A" 
		            +"  join "
		         +"( " 
		         +  " Select id BRecid from " + Tble2+ " "
		         +" ) B " 
		            +"  on"
		          +" ARecid=BRecid" 
		       +") ; ";
			
			FNsql="select count(*) as fn from "
		        +"("
					+"select opt.ARecid oa, opt.BRecid ob, r.ARecid ra, r.BRecid rb from " 
		      +"("
		      +" select   * from " 
		         +"( "
		         +"  Select id ARecid from "+ Tble1+ "  "
		         +") A" 
		            +"  join "
		         +"( " 
		         +  " Select id BRecid from " + Tble2+ "  "
		         +" ) B " 
		            +"  on"
		          +" ARecid=BRecid" 
		       +") opt "  
		          
		        +" Left Join "
		       + ph2ResultTble +" r "  
		          +" on "
		       +" concat(r.ARecid,r.BRecid) = concat(opt.ARecid,opt.BRecid) " 
		       
		      +" where r.ARecid is NULL  " 
			+") xx ;";

		try{
			stmt=conn.createStatement();
			ResultSet rs = stmt.executeQuery(FPsql);
			if (rs.next())
				Fp=rs.getInt("fp");
			
			rs = stmt.executeQuery(TPsql);
			if (rs.next())
				Tp=rs.getInt("tp");
			
			rs = stmt.executeQuery(FNsql);
			if (rs.next())
				Fn=rs.getInt("fn");
			
			rs = stmt.executeQuery(TotOrgMatchsql);
			if (rs.next())
				tot=rs.getInt("OrgTrueMatch");
			
			stmt.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
			//ExitNicely()
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
			//ExitNicely()
		
			
		}
		
		result[0]=Tp;
		result[1]=Fp;
		result[2]=Fn;
		result[3]=tot;
		
		return result;
	}
//=============================================================================================
	
	
	
					
					
//=========================================================================
	private void PhonMatch2Tables(Connection conn, String Tble1,
			String Tble2, String[] sigCNames, String PhnAlg) {
		
		String PhnMatchResultTble="NoNullPhn"+"_"+PhnAlg+"_"+"MatchResults";
		
		Statement stmt = null;
		String sqlAdd,sqlDrop,sql;
		String matchSQL;
		
		
		String[] SigsOfCNameOfTble1=new String[sigCNames.length];
		String[] SigsOfCNameOfTble2=new String[sigCNames.length];
		for(int i=0;i<sigCNames.length;i++){
			
			SigsOfCNameOfTble1[i]="NoNullPhn"+"_"+Tble1+sigCNames[i];
			SigsOfCNameOfTble2[i]="NoNullPhn"+"_"+Tble2+sigCNames[i];
		
		}
		
		try{
			
			
			sqlDrop="Drop Table If exists "+PhnMatchResultTble+";";
			sqlAdd = "CREATE TABLE if not exists "+PhnMatchResultTble+" ("+
					
					"ARecid int DEFAULT NULL,"+
					"BRecid int DEFAULT NULL"+
					
					
					") ;";
			//Alg={"Sndx","DMPhone","NYIIS"}
			
			if (PhnAlg.equals("DMPhone")){
				
				matchSQL=" select distinct A.ARecid,B.BRecid  from ((Select DISTINCT AFN.RecId ARecid,"
						+ " AFN.id Aid,  AFN.DmPhStr1 afnDm1, AFN.DmPhStr2 afnDm2,ALN.DmPhStr1 alnDm1,ALN.DmPhStr2 alnDm2 "
						+ "from "
						+ SigsOfCNameOfTble1[0]
						+ " as AFN Join "
						+ SigsOfCNameOfTble1[1]
						+ " as ALN ON AFN.id=ALN.id    ) order by afnDm1,afnDm2, alnDm1,alnDm2 ) A "
						+ " join "
						+ "((Select DISTINCT BFN.RecId BRecid, BFN.id Bid,BFN.DmPhStr1 bfnDm1,  "
					            	+ "BFN.DmPhStr2 bfnDm2,BLN.DmPhStr1 blnDm1,BLN.DmPhStr2 blnDm2 "
						+ "from "
						+ SigsOfCNameOfTble2[0]
						+ " as BFN Join "
						+ SigsOfCNameOfTble2[1]
						+ " as BLN ON BFN.id=BLN.id     )  order by bfnDm1,bfnDm2, blnDm1,blnDm2  ) B"
						+ "  ON "
						+ "  (A.afnDm1=B.bfnDm1 or  A.afnDm1=B.bfnDm2 or  A.afnDm2=B.bfnDm1 or  A.afnDm2=B.bfnDm2) and"
						+ "  (A.alnDm1=B.blnDm1 or  A.alnDm1=B.blnDm2 or  A.alnDm2=B.blnDm1 or  A.alnDm2=B.blnDm2)   ; " ;
				}
			
			//if (PhnAlg.equals("Sndx")){
			
			matchSQL=" select distinct A.ARecid,B.BRecid  from ((Select DISTINCT AFN.RecId ARecid,"
					+ " AFN.id Aid, AFN.SndxStr afnSndx,ALN.SndxStr alnSndx "
					+ "from "
					+ SigsOfCNameOfTble1[0]
					+ " as AFN Join "
					+ SigsOfCNameOfTble1[1]
					+ " as ALN ON AFN.id=ALN.id    ) order by afnSndx,alnSndx ) A "
					+ " join "
					+ "((Select DISTINCT BFN.RecId BRecid, BFN.id Bid,BFN.SndxStr bfnSndx, BLN.SndxStr blnSndx "
					+ "from "
					+ SigsOfCNameOfTble2[0]
					+ " as BFN Join "
					+ SigsOfCNameOfTble2[1]
					+ " as BLN ON BFN.id=BLN.id     )  order by bfnSndx,blnSndx  ) B"
					+ "  ON "
					+ " A.afnSndx=B.bfnSndx and A.alnSndx=B.blnSndx  ; " ;
			//}
			
			if (PhnAlg.equals("NYIIS")){
				
				matchSQL=" select distinct A.ARecid,B.BRecid  from ((Select DISTINCT AFN.RecId ARecid,"
						+ " AFN.id Aid, AFN.NyStr afnNys, ALN.NyStr alnNys "
						+ "from "
						+ SigsOfCNameOfTble1[0]
						+ " as AFN Join "
						+ SigsOfCNameOfTble1[1]
						+ " as ALN ON AFN.id=ALN.id    ) order by afnNys,alnNys ) A "
						+ " join "
						+ "((Select DISTINCT BFN.RecId BRecid, BFN.id Bid,BFN.NyStr bfnNys,BLN.NyStr blnNys "
						+ "from "
						+ SigsOfCNameOfTble2[0]
						+ " as BFN Join "
						+ SigsOfCNameOfTble2[1]
						+ " as BLN ON BFN.id=BLN.id     )  order by bfnNys,blnNys  ) B"
						+ "  ON "
						+ " A.afnNys=B.bfnNys and A.alnNys=B.blnNys  ; " ;
				}
			
			
			
			sql="INSERT INTO "+ PhnMatchResultTble +" "+ matchSQL;
			
			stmt = conn.createStatement();
			System.out.println("\n Deleting old Match result table ("+PhnMatchResultTble+ ") if exist ... ");
			stmt.executeUpdate(sqlDrop);
			System.out.println(" Ok !");
			System.out.println("\n Creating new Match result table("+PhnMatchResultTble+ ")  ... ");
			stmt.executeUpdate(sqlAdd);
			
			String sqlDropndx,sqlAddndx;
			String[] Indexes={"ARecid","BRecid","ARecid,BRecid"};
			
			for(int ndx=0;ndx<Indexes.length;ndx++){
				sqlDropndx="DROP INDEX  if exists "+PhnMatchResultTble+"ndx"+ndx;
				sqlAddndx="CREATE INDEX "+PhnMatchResultTble+"ndx"+ndx+
							  " ON " +PhnMatchResultTble+" USING btree ("+Indexes[ndx]+"); ";
				stmt.executeUpdate(sqlDropndx);
				stmt.executeUpdate(sqlAddndx);
			}
			
			System.out.println(" Ok !");
			System.out.println("\n Finding Matches, Please wait ...");
			stmt.executeUpdate(sql);
			System.out.println(" Completed !\n"); 

			stmt.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
			//ExitNicely()
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
			//ExitNicely()
		
			
		}//end try
		System.out.println("End of Phonetically Matching 2 Tables!");
		
	}

	//============================================================================================

	private int[] ComputePhonTpFpFn(Connection conn, String Tble1,
			String Tble2, String[] sigCNames, String PhnAlg) {
		int[] result=new int[4];
		int Tp=0,Fp=0,Fn=0,tot=0;
		
		Statement stmt = null;
		
		String TPsql,FPsql,FNsql,TotOrgMatchsql;
		
		
		String PhnMatchResultTble="NoNullPhn"+"_"+PhnAlg+"_"+"MatchResults";
				
		
		FPsql="select count(*) as fp from "+ PhnMatchResultTble+"  r where r.ARecid!=r.BRecid;";
		TPsql="select count(*) as tp from "+ PhnMatchResultTble+"  r where r.ARecid=r.BRecid;";
		TotOrgMatchsql=" select   count(*) as OrgTrueMatch from (" 
	         +"( "
	         +"  Select A1.id ARecid from "+ Tble1+ " A1  where A1."+sigCNames[0]+" !=\"\" and A1."+sigCNames[1]+"!=\"\" "
	         +") A" 
	            +"  join "
	         +"( " 
	         +  " Select B1.id BRecid from " + Tble2+ " B1  where B1." + sigCNames[0]+" !=\"\" and B1."+sigCNames[1]+"!=\"\" "
	         +" ) B " 
	            +"  on "
	          +" ARecid=BRecid" 
	       +") ; ";
		
		FNsql="select count(*) as fn from "
	        +"("
				+"select opt.ARecid oa, opt.BRecid ob, r.ARecid ra, r.BRecid rb from " 
	      +"("
	      +" select   * from " 
	         +"( "
	         +"  Select A1.id ARecid from "+ Tble1+ " A1  where A1."+sigCNames[0]+" !=\"\" and A1."+sigCNames[1]+"!=\"\" "
	         +") A" 
	            +"  join "
	         +"( " 
	         +  " Select B1.id BRecid from " + Tble2+ " B1  where B1." + sigCNames[0]+" !=\"\" and B1."+sigCNames[1]+"!=\"\" "
	         +" ) B " 
	            +"  on"
	          +" ARecid=BRecid" 
	       +") opt "  
	          
	        +" Left Join "
	       + PhnMatchResultTble +" r "  
	          +" on "
	       +" concat(r.ARecid,r.BRecid) = concat(opt.ARecid,opt.BRecid) " 
	       
	      +" where r.ARecid is NULL  " 
		+") xx;";

		try{
			stmt=conn.createStatement();
			ResultSet rs = stmt.executeQuery(FPsql);
			if (rs.next())
				Fp=rs.getInt("fp");
			
			rs = stmt.executeQuery(TPsql);
			if (rs.next())
				Tp=rs.getInt("tp");
			
			rs = stmt.executeQuery(FNsql);
			if (rs.next())
				Fn=rs.getInt("fn");
			
			rs = stmt.executeQuery(TotOrgMatchsql);
			if (rs.next())	
				tot=rs.getInt("OrgTrueMatch");
			
			stmt.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
			//ExitNicely()
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
			//ExitNicely()
		
			
		}
		
		result[0]=Tp;
		result[1]=Fp;
		result[2]=Fn;
		result[3]=tot;
		
		return result;
		
	}
	//============================================================================================
		
		public void Match2Tables(Connection conn,String Tble1,String Tble2, String[] SigCNames,
				int nGroups,int nGItems,boolean UseExtBiGrams,boolean Lcase) {
			
			
			
			Statement stmt = null;
			String sqlAdd,sqlDrop,sql;
			String matchSQL;
			char ExtBi=(UseExtBiGrams)? 'E':'N';
			
			String MatchResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults":"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults" ;
			
			
			String[] SigsOfCNameOfTble1=new String[SigCNames.length];
			String[] SigsOfCNameOfTble2=new String[SigCNames.length];
			for(int i=0;i<SigCNames.length;i++){
				SigsOfCNameOfTble1[i]=(Lcase)? "LNoNullSig":"NoNullSig";
				SigsOfCNameOfTble1[i]=SigsOfCNameOfTble1[i]+nGroups+"_"+nGItems+"_"+Tble1+ExtBi+SigCNames[i];
				
				SigsOfCNameOfTble2[i]=(Lcase)? "LNoNullSig":"NoNullSig"; 
				SigsOfCNameOfTble2[i]=SigsOfCNameOfTble2[i]+nGroups+"_"+nGItems+"_"+Tble2+ExtBi+SigCNames[i];
			
			}
			
			try{
				
				
				sqlDrop="Drop Table If exists "+MatchResultTble+";";
				sqlAdd = "CREATE TABLE if not exists "+MatchResultTble+" ("+
						
						"ARecid int DEFAULT NULL,"+
						"BRecid int DEFAULT NULL"+
						
						
						") ;";

				matchSQL=" select distinct A.ARecid,B.BRecid  from "
					+"((Select DISTINCT "
					+ "AFN.RecId ARecid,"
					+ "AFN.id Aid,"
					+ "AFN.SigId ASigid,"
					+ "AFN.Sig afnSig,"
					+ "ALN.Sig alnSig "
					+ " from " + SigsOfCNameOfTble1[0]+" as AFN Join "+	SigsOfCNameOfTble1[1]
							+" as ALN ON AFN.id=ALN.id ) order by afnSig,alnSig,ASigId ) A" 
	                 +" join " 
	                  +"((Select DISTINCT "
	                  + "BFN.RecId BRecid, "
	                  + "BFN.id Bid, "
	                  + "BFN.SigId BSigid, "
	                  + "BFN.Sig bfnSig, "
	                  + "BLN.Sig blnSig "
	                  +" from " + SigsOfCNameOfTble2[0]+" as BFN Join "+	SigsOfCNameOfTble2[1]
	                  +" as BLN ON BFN.id=BLN.id     )  order by bfnSig,blnSig,BSigId   ) B"
	                  
	                  +" ON " 
	                  
	                  +" A.afnSig=B.bfnSig and A.alnSig=B.blnSig and A.ASigId=B.BSigid  ;" ;
				
				
				
				
				sql="INSERT INTO "+ MatchResultTble +" "+ matchSQL;
				
				stmt = conn.createStatement();
				System.out.println("\n Deleting old Match result table ("+MatchResultTble+ ") if exist ... ");
				stmt.executeUpdate(sqlDrop);
				System.out.println(" Ok !");
				System.out.println("\n Creating new Match result table("+MatchResultTble+ ")  ... ");
				stmt.executeUpdate(sqlAdd);
				
				String sqlDropndx,sqlAddndx;
				String[] Indexes={"ARecid","BRecid","ARecid,BRecid"};
				
				for(int ndx=0;ndx<Indexes.length;ndx++){
					sqlDropndx="DROP INDEX  if exists  "+MatchResultTble+"ndx"+ndx;
					sqlAddndx="CREATE INDEX "+MatchResultTble+"ndx"+ndx+
								  " ON " +MatchResultTble+" USING btree ("+Indexes[ndx]+"); ";
					stmt.executeUpdate(sqlDropndx);
					stmt.executeUpdate(sqlAddndx);
				}
				
				System.out.println(" Ok !");
				System.out.println("\n Finding Matches, Please wait ...");
				stmt.executeUpdate(sql);
				System.out.println(" Completed !\n"); 

				stmt.close();
			}catch(SQLException se){
				//Handle errors for JDBC
				se.printStackTrace();
				//ExitNicely()
			}catch(Exception e){
				//Handle errors for Class.forName
				e.printStackTrace();
				//ExitNicely()
			
				
			}//end try
			System.out.println("End of Matching 2 Tables!");
		}//end match2tables


		//===========================================================================================
		// new match 2 tables that calls join2tables
		
		
public void MultiPhaseMatch2TablesOnSingleColumn(Connection conn,String Tble1,String Tble2, String[] SigCNames,
					int nGroups,int nGItems,boolean UseExtBiGrams,boolean Lcase, String[] ExtCols) {
				
				// we assume the main Sig. Col names are the 1st and 2nd in SigCNames
				// Other Col names are considered extra, So the result will be built on the 1st and 2nd 
				//then the extra cols are added in the result as a true (match) or false (not match)   
				
				
				char ExtBi=(UseExtBiGrams)? 'E':'N';
				
				String[] ph1ResultTbles=new String[SigCNames.length]; 
				
				String ph2ResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"Ph2MatchResults"+SigCNames[0]+"_"+SigCNames[1]:
					"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+"Ph2MatchResults"+SigCNames[0]+"_"+SigCNames[1] ;
				
				String[] SigsOfCNameOfTble1=new String[SigCNames.length];
				String[] SigsOfCNameOfTble2=new String[SigCNames.length];
				for(int i=0;i<SigCNames.length;i++){
					SigsOfCNameOfTble1[i]=(Lcase)? "LNoNullSig":"NoNullSig";
					SigsOfCNameOfTble1[i]=SigsOfCNameOfTble1[i]+nGroups+"_"+nGItems+"_"+Tble1+ExtBi+SigCNames[i];
					
					SigsOfCNameOfTble2[i]=(Lcase)? "LNoNullSig":"NoNullSig"; 
					SigsOfCNameOfTble2[i]=SigsOfCNameOfTble2[i]+nGroups+"_"+nGItems+"_"+Tble2+ExtBi+SigCNames[i];
					
					ph1ResultTbles[i]=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+SigCNames[i]+"MatchResults":
						               "NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+SigCNames[i]+"MatchResults" ;
				}
				
				
				
				
					String[] t1selectCols, t2selectCols,T1SelAliaces, T2SelAliaces,T1T2CondCols,CondOps,CondJunc;
					
					  // phase 1 means first stage of matching, i.e. matching firstnames
					 t1selectCols=new String[] {"RecId"};    //[0].equals("RecId");
					 t2selectCols=new String[] {"RecId"};     //   [0].equals("RecId");
					 T1SelAliaces=new String[]{"ARecId"};
					 T2SelAliaces=new String[]{"BRecId"};
					 T1T2CondCols=new String[]{"Sig","SigId"};
					 CondOps =new String[]{"=","="};
					 CondJunc=new String[]{"and"};
				
					//phase 1
					int res=0;
					for(int i=0;i<SigCNames.length;i++){	
						
					  res=Join2TablesByNameOnCondition( conn,SigsOfCNameOfTble1[i], SigsOfCNameOfTble2[i],t1selectCols,t2selectCols,
							T1SelAliaces ,T2SelAliaces,ph1ResultTbles[i], T1T2CondCols, CondOps , CondJunc); 
					
					}
					
					//phase 2
					if(res==1){

						 t1selectCols=new String[]{"ARecId","BRecId"};
						 // we just need first table columns ...t2selectCols=new String[]{"ARecId","BRecId"};
						 t2selectCols=null; 
						 T1SelAliaces= new String[]{"ARecId","BRecId"};
						 // not needed T2SelAliaces=new String[]{"ARecId","BRecId"};
						 T1T2CondCols=new String[]{"ARecId","BRecId"};
						 CondOps =new String[]{"=","="};
						 CondJunc=new String[]{"and"};
					
						 res=Join2TablesByNameOnCondition( conn,ph1ResultTbles[0], ph1ResultTbles[1],t1selectCols,t2selectCols,
							T1SelAliaces ,T2SelAliaces,ph2ResultTble, T1T2CondCols, CondOps , CondJunc);
					}
					
					
					
				// phase 3: add ext col match to ph2 result	

			if(res==1 && ExtCols!=null){		
				for(int j=0;j<ExtCols.length;j++){
					
					String NewColMatchTbl=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+ExtCols[j]+"MatchResults":
			               "NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+ExtCols[j]+"MatchResults" ;
				AddToPh2ResultNewMatchedCol(conn, ph2ResultTble,NewColMatchTbl, "M_"+ExtCols[j]);	
				}
			}	
				
				
				System.out.println("End of Matching 2 Tables based on single column!");
		
		}//end match2tables

			

//============================================================================================
	// Add a new column match
	
	
public void AddToPh2ResultNewMatchedCol(Connection conn,String ph2ResultTbl,String NewColMatchTbl,String NewCol){
	String AddColsql="ALTER TABLE "+ph2ResultTbl+"  ADD COLUMN "+ NewCol+"  boolean;";
	Statement stmt = null;
	boolean ColExist=CheckIfColExistInTable(conn,ph2ResultTbl,NewCol);
	String UpdtSql=" UPDATE "+ ph2ResultTbl+"  t SET "
			+NewCol + " =(SELECT count(*)>0  FROM (Select t1.ARecid,t1.BRecid from "
					+ ph2ResultTbl+" t1 left JOIN "
					        +NewColMatchTbl+" t2 on  t1.ARecid=t2.ARecid and t1.BRecid=t2.BRecid"
							+ " where t2.ARecid is not NULL) fncob"
					+ " WHERE t.ARecid=fncob.ARecid AND t.BRecid=fncob.BRecid);" ;
	
	try{
		
		stmt = conn.createStatement();
	if(!ColExist)
	{
		stmt.executeUpdate(AddColsql);	
	}
	
	
	stmt.executeUpdate(UpdtSql);	

	stmt.close();
}catch(SQLException se){
	//Handle errors for JDBC
	se.printStackTrace();
	//ExitNicely()
}//end try
}

//========================================================================================			
private boolean CheckIfColExistInTable(Connection conn, String ph2ResultTbl,
		String newCol) {
	
	
	try {
		
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getColumns(null, null, ph2ResultTbl, newCol);
		 if (rs.next()) {
		      return(true); //Column in table exist
		    }
		
	} catch (SQLException e) {
		// Auto-generated catch block
		e.printStackTrace();
	}
	
	return false;
}


//============================================================================================	
		// I need to create a match2tables function for single column
		// compare the result and speed  when you match single columns then combine them with matching multible columns 
		public void Match2TablesOnSingleColumn0(Connection conn,String Tble1,String Tble2, String SigCName,
				int nGroups,int nGItems,boolean UseExtBiGrams,boolean Lcase) {
			
			
			
			Statement stmt = null;
			String sqlAdd,sqlDrop,sql;
			String matchSQL;
			char ExtBi=(UseExtBiGrams)? 'E':'N';
			
			String MatchResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+SigCName+"MatchResults":"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"_"+SigCName+"MatchResults" ;
			
			
			String SigsOfCNameOfTble1;
			String SigsOfCNameOfTble2;
			
				SigsOfCNameOfTble1=(Lcase)? "LNoNullSig":"NoNullSig";
				SigsOfCNameOfTble1=SigsOfCNameOfTble1+nGroups+"_"+nGItems+"_"+Tble1+ExtBi+SigCName;
				
				SigsOfCNameOfTble2=(Lcase)? "LNoNullSig":"NoNullSig"; 
				SigsOfCNameOfTble2=SigsOfCNameOfTble2+nGroups+"_"+nGItems+"_"+Tble2+ExtBi+SigCName;
			
			
			
			try{
				
				
				sqlDrop="Drop Table If exists "+MatchResultTble+";";
				sqlAdd = "CREATE TABLE if not exists "+MatchResultTble+" ("+
						
						"ARecid int DEFAULT NULL,"+
						"BRecid int DEFAULT NULL"+
						
						//"INDEX "+MatchResultTble+"ndx (ARecId,BRecId)"+
						");";

				matchSQL=" select distinct A.RecId ARecid,B.RecId BRecid  from "
					 + SigsOfCNameOfTble1 +" A" //+"  order by A.Sig,A.SigId as A" 
	                 +" join " 
	                  + SigsOfCNameOfTble2  +" B" //+"   order by B.Sig,B.SigId   as B"
	                  
	                  +" ON " 
	                  
	                  +" A.Sig=B.Sig and A.SigId=B.Sigid  ;" ;
				
				
				
				
				sql="INSERT INTO "+ MatchResultTble +" "+ matchSQL;
				
				stmt = conn.createStatement();
				System.out.println("\n Deleting old Match result table ("+MatchResultTble+ ") if exist ... ");
				stmt.executeUpdate(sqlDrop);
				System.out.println(" Ok !");
				System.out.println("\n Creating new Match result table("+MatchResultTble+ ")  ... ");
				stmt.executeUpdate(sqlAdd);
				

				String sqlDropndx,sqlAddndx;
				String[] Indexes={"ARecid","BRecid","ARecid,BRecid"};
				
				for(int ndx=0;ndx<Indexes.length;ndx++){
					sqlDropndx="DROP INDEX  if exists "+MatchResultTble+"ndx"+ndx;
					sqlAddndx="CREATE INDEX "+MatchResultTble+"ndx"+ndx+
								  " ON " +MatchResultTble+" USING btree ("+Indexes[ndx]+"); ";
					stmt.executeUpdate(sqlDropndx);
					stmt.executeUpdate(sqlAddndx);
				}
				
				System.out.println(" Ok !");
				System.out.println("\n Finding Matches, Please wait ...");
				stmt.executeUpdate(sql);
				System.out.println(" Completed !\n"); 

				stmt.close();
			}catch(SQLException se){
				//Handle errors for JDBC
				se.printStackTrace();
				//ExitNicely()
			}catch(Exception e){
				//Handle errors for Class.forName
				e.printStackTrace();
				//ExitNicely()
			
				
			}//end try
			System.out.println("End of Matching 2 Tables based on single column!");
		}//end match2tables

		
		
		
		
//=========================================================================	
	
//===============================================================================
		
		//joins any 2  tables (phase 1 or phase 2)  based on condition
		// more general , join two tables by name on condition
public int Join2TablesByNameOnCondition(Connection conn1,String Tble1Name,String Tble2Name,
				     String[] t1selectCols,String[] t2selectCols, String[] T1SelAliaces,
						String[] T2SelAliaces,String ResultTble, String[] T1T2CondCols,String[] CondOps ,String[] CondJunc) {
			
			
			
			Statement stmt = null;
			String sqlAdd,sqlDrop,sql;
			String matchSQL;
			
			String cond="";
			String SelPhrase="",SelPhrase2="";
			//String[] T1SelAliaces=new String[t1selectCols.length];
			//String[] T2SelAliaces=new String[t2selectCols.length];
			
			String t1Aliace="A",t2Aliace="B"; 
			
			for(int i=0;i<T1T2CondCols.length;i++){
				String tcond=t1Aliace+"."+T1T2CondCols[i]+CondOps[i]+t2Aliace+"."+T1T2CondCols[i]; // condition term
				if(i<T1T2CondCols.length-1)
					tcond=tcond+" "+CondJunc[i]+" ";
				cond=cond+tcond;
			}
			
			
			for(int i=0;i<t1selectCols.length;i++){
				//T1SelAliaces[i]=t1Aliace+t1selectCols[i];
				String tph=t1Aliace+"."+t1selectCols[i]+" "+T1SelAliaces[i];
				if (i< t1selectCols.length-1)
					tph=tph+",";
				SelPhrase=SelPhrase+tph;
				
			}
			if(t2selectCols!=null)
			for(int i=0;i<t2selectCols.length;i++){
				//T2SelAliaces[i]=t2Aliace+t2selectCols[i];
				String tph=t2Aliace+"."+t2selectCols[i]+" "+T2SelAliaces[i];
				if (i< t2selectCols.length-1)
					tph=tph+",";
				SelPhrase2=SelPhrase2+tph;
				
			}
			if(SelPhrase2.length()>0){
				if(SelPhrase.length()>0)
					SelPhrase=SelPhrase+","+SelPhrase2;
				else
					SelPhrase=SelPhrase2;
			}
			
			
			
			try{
				
				
				sqlDrop="Drop Table If exists "+ResultTble+";";
				sqlAdd = "CREATE TABLE if not exists "+ResultTble+" ("+
						
						"ARecid int DEFAULT NULL,"+
						"BRecid int DEFAULT NULL"+
						
						//"INDEX "+MatchResultTble+"ndx (ARecId,BRecId)"+
						") ;";
				 
				 //distinct removed
				matchSQL=" select  "+SelPhrase +"  from "
					 + Tble1Name +"  "+ t1Aliace //+"  order by A.Sig,A.SigId as A" 
	                 +" join " 
	                  + Tble2Name  +"  "+ t2Aliace //+"   order by B.Sig,B.SigId   as B"
	                  
	                  +" ON " 
	                  
	                  +cond+"  ;" ;
				
				
				
				
				sql="INSERT INTO "+ ResultTble +" "+ matchSQL;
				
				stmt = conn1.createStatement();
				System.out.println("\n Deleting old Match result table ("+ResultTble+ ") if exist ... ");
				stmt.executeUpdate(sqlDrop);
				System.out.println(" Ok !");
				System.out.println("\n Creating new Match result table("+ResultTble+ ")  ... ");
				stmt.executeUpdate(sqlAdd);
				

				String sqlDropndx,sqlAddndx;
				String[] Indexes={"ARecid","BRecid","ARecid,BRecid"};
				
				for(int ndx=0;ndx<Indexes.length;ndx++){
					sqlDropndx="DROP INDEX  if exists  "+ResultTble+"ndx"+ndx;
					sqlAddndx="CREATE INDEX "+ResultTble+"ndx"+ndx+
								  " ON " +ResultTble+" USING btree ("+Indexes[ndx]+"); ";
					stmt.executeUpdate(sqlDropndx);
					stmt.executeUpdate(sqlAddndx);
				}
				System.out.println(" Ok !");
				System.out.println("\n Finding Matches, Please wait ...");
				
				
				stmt.executeUpdate(sql);
				System.out.println(" Completed !\n"); 

				stmt.close();
			}catch(SQLException se){
				//Handle errors for JDBC
				se.printStackTrace();
				
				//ExitNicely()
			}catch(Exception e){
				//Handle errors for Class.forName
				e.printStackTrace();
				//ExitNicely()
			
				return 0;
			}//end try
			System.out.println("End ofJoin2TablesByNameOnCondition!");
			return 1;
		}//end match2tables

				

	
	
//=========================================================================

				
				
				
public int[] ComputeTpFpFn(Connection conn,String Tble1,String Tble2, String[] SigCNames,
		int nGroups,int nGItems,boolean UseExtBiGrams,boolean Lcase) {
	int[] result=new int[4];
	int Tp=0,Fp=0,Fn=0,tot=0;
	
	Statement stmt = null;
	
	String TPsql,FPsql,FNsql,TotOrgMatchsql;
	
	char ExtBi=(UseExtBiGrams)? 'E':'N';
	
	String MatchResultTble=(Lcase)? "LNoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults":"NoNullSig"+nGroups+"_"+nGItems+"_"+ExtBi+"MatchResults" ;
	
	FPsql="select count(*) as fp from "+ MatchResultTble+"  r where r.ARecid!=r.BRecid;";
	TPsql="select count(*) as tp from "+ MatchResultTble+"  r where r.ARecid=r.BRecid;";
	TotOrgMatchsql=" select   count(*) as OrgTrueMatch from ( " 
         +"( "
         +"  Select A1.id ARecid from "+ Tble1+ " A1  where A1."+SigCNames[0]+" !=\"\" and A1."+SigCNames[1]+"!=\"\" "
         +") A" 
            +"  join "
         +"( " 
         +  " Select B1.id BRecid from " + Tble2+ " B1  where B1." + SigCNames[0]+" !=\"\" and B1."+SigCNames[1]+"!=\"\" "
         +" ) B " 
            +"  on"
          +" ARecid=BRecid" 
       +") ; ";
	
	FNsql="select count(*) as fn from "
        +"("
			+"select opt.ARecid oa, opt.BRecid ob, r.ARecid ra, r.BRecid rb from " 
      +"("
      +" select   * from " 
         +"( "
         +"  Select A1.id ARecid from "+ Tble1+ " A1  where A1."+SigCNames[0]+" !=\"\" and A1."+SigCNames[1]+"!=\"\" "
         +") A" 
            +"  join "
         +"( " 
         +  " Select B1.id BRecid from " + Tble2+ " B1  where B1." + SigCNames[0]+" !=\"\" and B1."+SigCNames[1]+"!=\"\" "
         +" ) B " 
            +"  on"
          +" ARecid=BRecid" 
       +") opt "  
          
        +" Left Join "
       + MatchResultTble +" r "  
          +" on "
       +" concat(r.ARecid,r.BRecid) = concat(opt.ARecid,opt.BRecid) " 
       
      +" where r.ARecid is NULL  " 
	+") xx ;";

	try{
		stmt=conn.createStatement();
		ResultSet rs = stmt.executeQuery(FPsql);
		if (rs.next())
			Fp=rs.getInt("fp");
		
		rs = stmt.executeQuery(TPsql);
		if (rs.next())
			Tp=rs.getInt("tp");
		
		rs = stmt.executeQuery(FNsql);
		if (rs.next())
			Fn=rs.getInt("fn");
		
		rs = stmt.executeQuery(TotOrgMatchsql);
		if (rs.next())
			tot=rs.getInt("OrgTrueMatch");
		
		stmt.close();
	}catch(SQLException se){
		//Handle errors for JDBC
		se.printStackTrace();
		//ExitNicely()
	}catch(Exception e){
		//Handle errors for Class.forName
		e.printStackTrace();
		//ExitNicely()
	
		
	}
	
	result[0]=Tp;
	result[1]=Fp;
	result[2]=Fn;
	result[3]=tot;
	
	return result;
}
				
				
//=========================================================================



}//end class




















