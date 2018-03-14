package dbprocess;
import java.awt.EventQueue;

//STEP 1. Import required packages

import java.awt.Frame;
//import java.awt.EventQueue;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;

import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import probmatch.HashSig;
import probmatch.Phonetics;
//import bin.ProbMatch.HashSig;

import clientconfig.Config;
import clientconfig.GetConfig;

import clientconfig.SysConfigMainWindow;

public class DBConnect {
	// JDBC driver name and database URL
	//static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String JDBC_DRIVER="org.postgresql.Driver";
	//static final String DB_URL = "jdbc:mysql://localhost/TestData";
	static  String DB_URL = "jdbc:postgresql://localhost/TestData";

	//jdbc:postgresql:database
	//jdbc:postgresql://host/database
	//jdbc:postgresql://host:port/database


	//  Database credentials
	static  String USER = "user1";
	static  String PASS = "User1";
	static  String Timesfilename= "TimingResults2.txt";
	static  String Fpfilename= "FpResults.txt";
	static  PrintWriter Rout = null;
	static PrintWriter FpRout = null;




	public static void main(String[] args) {
		//JOptionPane.showMessageDialog(null, "Message goes here");
		DBConnect db1=new DBConnect();

		Config CurrentConf=new Config();

		SetConfiguration(CurrentConf);

		//GetConfig.mainnnn( CurrentConf);
		//PasswordDemo.mainnn();


		//boolean newSession=false; 
		//boolean newSession=true;
		// newSession For doing the matching processes again, if not then just compute Fp,Fn,Tp
		//StartOver to create the SigTables again
		boolean startOver=true;
		long endTime ;
		long duration;
		long startTime;
		Calendar cal=Calendar.getInstance();

		try {
			Rout = new PrintWriter(new BufferedWriter(new FileWriter(Timesfilename, true)));
			Rout.println("Begin Timing:");
			FpRout = new PrintWriter(new BufferedWriter(new FileWriter(Fpfilename, true)));
			FpRout.println("\nBegin New Test:"+cal.getTime());



			if(startOver){	    
				startTime = System.currentTimeMillis();  //nanoTime();

				//1) Create Sig tables	
				Rout.println("Create Sig For MyTables for ExtBig=True  Started @:"+startTime);  

				db1.CreateSigforMyTable(CurrentConf);

				endTime = System.currentTimeMillis();
				duration = (endTime - startTime);
				Rout.println("\n-----------------------------------------");
				Rout.println("\n Ended @:"+endTime +" And lasted for : "+duration);
				System.out.println("\n This took : "+duration);

			}



		}catch (IOException e) {
			System.err.println(e);
		}finally{
			if(Rout != null)
				Rout.close();
			if(FpRout != null)
				FpRout.close();


		} 	

	}


	//=============================================================================================
	/* public static void SetConfiguration( final Config CurrentConfig)
	    {
	        SwingUtilities.invokeLater(new Runnable()
	        {
	            public void run()
	            {
	            	SysConfigMainWindow.createAndShowGUI(CurrentConfig);
	            }
	        });
	        while(!CurrentConfig.dataSaved);
	    }

	 */

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

			//for(;frame.isShowing();) System.out.println();
			/*while(frame.isShowing()){//!CurrentConfig.dataSaved
						int i=0;//System.out.println(!CurrentConfig.dataSaved);

					}*/
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	/*public static void SetConfiguration( final Config CurrentConfig) throws InvocationTargetException, InterruptedException {
	EventQueue.invokeAndWait(new Runnable() {
		public void run() {
			try {
				SysConfigMainWindow frame = new SysConfigMainWindow(CurrentConfig);

				frame.setVisible(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	});
}*/




	//==================================================================================
	/////////////////////////////////
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
		System.out.println("min r="+min+"  imin="+imin);
		return (imin);	
	}	
	//=========================================================================	
	public void CreateSigforMyTable(Config CurrentConf) {
		Connection conn = null;

		String InTable = CurrentConf.InTable;

		String[] LinkageColumnsToMakeSigs =CurrentConf.LinkageColumnsToMakeSigs;
		String[] LinkColsOfSigsToRecs = CurrentConf.LinkColsOfSigsToRecs;
		// for broker ...String[] ColumnsUsed4CalculatingMatchTR = CurrentConf.ColumnsUsed4CalculatingMatchTR;
		int nHashes = CurrentConf.nHashes;

		double DefaultSigSimilarityTreshold = CurrentConf.SigSimilarityTreshold;
		//double SimTRforEachCol[]=CurrentConf.SimTRforEachColumn;

		//for broker ... double minTotColsWeight4Match = CurrentConf.minTotColsWeight4Match;


		USER = CurrentConf.User;
		PASS=CurrentConf.Pass;
		DB_URL=CurrentConf.DBurl;

		boolean UseExtBiGrams=CurrentConf.UseExtBiGrams;
		boolean UsePhon=CurrentConf.UsePhon;


		int EachColnGroups[]=new int[CurrentConf.SimTRforEachColumn.length];
		int EachColnGItems[]=new int[CurrentConf.SimTRforEachColumn.length];
		for(int i=0;i<CurrentConf.SimTRforEachColumn.length;i++){
			EachColnGItems[i]= calnGItems(nHashes,CurrentConf.SimTRforEachColumn[i]);
			EachColnGroups[i]=nHashes/EachColnGItems[i];
		}


		System.out.println("\n nGroups=" +Arrays.toString(EachColnGroups));
		System.out.println("\n nGitems=" +Arrays.toString(EachColnGItems));
		try{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);
			//Class.forName("org.postgresql.Driver");
			//STEP 3: Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);


			if (UsePhon){

				System.out.println("\nCreating  Phonitics Table for "+InTable+"...");
				long startTime = System.currentTimeMillis();

				int CrtSigTbl=CreatePhonTableMultiSig( conn, InTable,LinkageColumnsToMakeSigs,LinkColsOfSigsToRecs);
				long endTime = System.currentTimeMillis();
				long duration = (endTime - startTime);
				if (CrtSigTbl!=0){
					System.out.println("\n Phonitics Table for "+InTable+"_"+LinkageColumnsToMakeSigs[0]+" Created Successfully ...");
					System.out.println("\n This took : "+duration);
					Rout.println("\n Creating Phonitics Table for "+InTable+" Took :"+duration);
				} else
				{System.err.println("Error! creating Phonitics Table for column "+InTable+"_"+LinkageColumnsToMakeSigs[0]+" ...\n");

				}

			} // if usePhon


			System.out.println("Starting creating Sig. Table for Table "+InTable+ " ...");
			//for(int j=0;j<nGroups.length;j++){
			long startTime = System.currentTimeMillis();  //nanoTime();

			int CrtSigTbl=CreateSigTableMultiSig( conn, InTable,LinkageColumnsToMakeSigs,LinkColsOfSigsToRecs,
					EachColnGroups, EachColnGItems, UseExtBiGrams);
			long endTime = System.currentTimeMillis();
			long duration = (endTime - startTime);

			if (CrtSigTbl!=0){
				System.out.println("\n"+Arrays.toString(LinkageColumnsToMakeSigs)+" With Sim. TRs( "+Arrays.toString(CurrentConf.SimTRforEachColumn)
				+")\n Sig Tables for Table "+InTable+"_"+" BiGrams="+UseExtBiGrams+", Created Successfully ...");
				//int calacSig=CalcSigforTable(  conn, Tble, SigCNames, SigCSizes , SelectedColsNamesA, nGroups, nGItems, UseExtBiGrams);
				System.out.println("\n This took : "+duration);
				Rout.println("\n Creating Sig Table for "+InTable+" Took :"+duration);
			} else
			{System.err.println("Error! creating Sig Table for column "+InTable+"_"+LinkageColumnsToMakeSigs[0]+UseExtBiGrams+" ...\n");

			}
			//}//for j

			//}//for i


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
		System.out.println("End of CreateSigforMyTables!");
	}//end main




	//========================================================================================			
	/*private boolean CheckIfColExistInTable(Connection conn, String ph2ResultTbl,
		String newCol) {


	try {

		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getColumns(null, null, ph2ResultTbl, newCol);
		 if (rs.next()) {
		      return(true); //Column in table exist
		    }

	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	return false;
}

	 */
	//============================================================================================	

	//=========================================================================
	//Create Sig. table for certain field (column)
	public int CreateSigTable(Connection conn,String Tble, String SigCNames,  String[]  SigRecLinkFieldNames,
			int nGroups,int nGItems,boolean UseExtBiGrams)
	{
		String sqlAdd,sqlDrop,sql,sqlIns;
		Statement stmt = null,stmt2=null;
		PreparedStatement stmt3=null;
		String SigCNameTbl="Sig"+nGroups+"_"+nGItems+"_"+Tble+UseExtBiGrams;

		SigCNameTbl=SigCNameTbl+SigCNames;
		long[] Sig1;
		sqlIns="Insert into "+SigCNameTbl+" (RecId,SigId,SIg) Values (?,?,?) ;";
		sqlDrop="Drop Table If exists "+SigCNameTbl+";";
		sqlAdd = "CREATE TABLE if not exists "+SigCNameTbl+" ("+
				"id SERIAL,"+
				"RecId int DEFAULT NULL,"+
				"SigId int DEFAULT NULL,"+
				"Sig bigint DEFAULT NULL,"+
				"PRIMARY KEY (id) , "+
				// not working with postgresql "INDEX "+SigCNameTbl+"ndx (SIg,SigId)"+
				");"; // ENGINE=MyISAM DEFAULT CHARSET=latin1;";

		try {
			stmt = conn.createStatement();

			stmt.executeUpdate(sqlDrop);
			stmt.executeUpdate(sqlAdd);

			HashSig hs1= new HashSig( nGroups,	nGItems);
			//Sig1= new long[nGroups];

			sql="SELECT * from "+ Tble;

			stmt2 = conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			//: Execute a query

			ResultSet rs = stmt2.executeQuery(sql);


			// Loop through result set and add Sig
			//Move to BFR position so while-loop works properly
			rs.beforeFirst();
			sqlIns="Insert into "+SigCNameTbl+" (RecId,SigId,SIg) Values (?,?,?) ;";
			stmt3=conn.prepareStatement(sqlIns);
			String Str1="";
			while(rs.next()){
				//Retrieve by column name


				Str1 = rs.getString(SigCNames);
				if ( (Str1!=null ) && (Str1.length()>0)) {
					int RecId=rs.getInt(SigRecLinkFieldNames[0]);

					Sig1=hs1.GetSig(Str1, 2,UseExtBiGrams,false);
					for(int i=0;i<Sig1.length;i++){

						stmt3.setInt(1, RecId);  // Link field as FK to org. table
						stmt3.setInt(2, i+1); //Sig Id is its index
						stmt3.setLong(3, Sig1[i]);
						stmt3.addBatch();
					}

					int[] rows = stmt3.executeBatch();
					if(rows[0]<1) System.err.println("Failed to add Sig. row to table "+SigCNameTbl);
				}		

			}

			stmt.close();
			stmt2.close();
			stmt3.close();
		} catch (SQLException e) {

			System.err.println("Failed to create Sig. Table "+SigCNameTbl);
			e.printStackTrace();
			return 0;
		} 

		return 1;
	}

	//=========================================================================
	//=========================================================================
	//Create Sig. table for certain field (column)
	public int CreateSigTableMultiSig(Connection conn,String Tble, String[] SigCNames,  String[]  SigRecLinkFieldNames,
			int[] nGroups,int[] nGItems,boolean UseExtBiGrams)
	{
		String sqlAdd,sqlDrop,sql,sqlIns,sqlAddndx,sqlDropndx;
		String[] Indexes={"sig, sigid","sig","recid"};
		char ExtBi=(UseExtBiGrams)? 'E':'N';

		//String[] sqlIns=new String[SigCNames.length];
		Statement stmt = null,stmt2=null;
		PreparedStatement stmt3[]=new PreparedStatement[SigCNames.length];
		String SigCNameTbl= "NoNullSig"+nGroups[0]+"_"+nGItems[0]+"_"+Tble+ExtBi;

		long[] Sig1;
		try {
			stmt = conn.createStatement();

			sql="SELECT * from "+ Tble;

			stmt2 = conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			//: Execute a query

			ResultSet rs = stmt2.executeQuery(sql);




			for(int i=0;i<SigCNames.length;i++){

				SigCNameTbl="NoNullSig"+nGroups[i]+"_"+nGItems[i]+"_"+Tble+ExtBi+SigCNames[i];

				//sqlIns="Insert into "+SigCNameTbl+" (RecId,SigId,SIg) Values (?,?,?) ;";
				sqlDrop="Drop Table If exists "+SigCNameTbl+";";
				sqlAdd = "CREATE TABLE if not exists "+SigCNameTbl+" ("+
						"id SERIAL,"+
						"RecId int DEFAULT NULL,"+
						"SigId int DEFAULT NULL,"+
						"Sig bigint DEFAULT NULL, "+
						"CONSTRAINT "+SigCNameTbl+"PK"+" PRIMARY KEY (sig, sigid, recid)"+

							") ;";




				stmt.executeUpdate(sqlDrop);
				stmt.executeUpdate(sqlAdd);
				for(int ndx=0;ndx<Indexes.length;ndx++){
					sqlDropndx="DROP INDEX if exists "+SigCNameTbl+"ndx"+ndx;
					sqlAddndx="CREATE INDEX "+SigCNameTbl+"ndx"+ndx+
							" ON " +SigCNameTbl+" USING btree ("+Indexes[ndx]+"); ";
					stmt.executeUpdate(sqlDropndx);
					stmt.executeUpdate(sqlAddndx);
				}


			}


			HashSig[] hs1=new HashSig[SigCNames.length] ;
			for(int i=0;i<SigCNames.length;i++)
				hs1[i]= new HashSig( nGroups[i],nGItems[i]);
			//Sig1= new long[nGroups];

			// Loop through result set and add Sig
			//Move to BFR position so while-loop works properly

			//boolean goodTuple=true;
			int bat=0;
			rs.last();
			int size= rs.getRow();
			rs.beforeFirst();
			String[] Str1=new String[SigCNames.length];

			for(int i=0;i<SigCNames.length;i++){
				SigCNameTbl="NoNullSig"+nGroups[i]+"_"+nGItems[i]+"_"+Tble+ExtBi+SigCNames[i];
				//SigCNameTbl=LC+"NoNullSig"+nGroups+"_"+nGItems+"_"+Tble+ExtBi+SigCNames[i];
				sqlIns="Insert into "+SigCNameTbl+" (RecId,SigId,SIg) Values (?,?,?) ;";
				stmt3[i]=conn.prepareStatement(sqlIns);
			}

			while(rs.next()){
				//	goodTuple=true;
				bat++;
				int RecId=rs.getInt(SigRecLinkFieldNames[0]);

				//Retrieve by column name
				/* this is for removing tubles have missing value in any column
					for(int i=0;i<SigCNames.length;i++){
							Str1[i]= rs.getString(SigCNames[i]);
							if ( (Str1[i] ==null ) || (Str1[i].length()==0)) 
								goodTuple=false;
					}	*/
				// So we want just remove the missing value of this column
				for(int i=0;i<SigCNames.length;i++){
					Str1[i]= rs.getString(SigCNames[i]);
				}


				//if(goodTuple){

				for(int i=0;i<SigCNames.length;i++){

					//for (int j=0;j<SigCNames.length;j++){
					//if (j>0) Str1=Str1+" ";
					//	Str1 =Str1+ rs.getString(SigCNames[j]);
					//}

					if ( (Str1[i] !=null ) && (Str1[i].length()!=0))
					{
						Sig1=hs1[i].GetSig(Str1[i], 2,UseExtBiGrams,false);
						for(int k=0;k<Sig1.length;k++){

							stmt3[i].setInt(1, RecId);  // Link field as FK to org. table
							stmt3[i].setInt(2, k+1); //Sig Id is its index
							stmt3[i].setLong(3, Sig1[k]);
							stmt3[i].addBatch();
						}
					} //if
					// else skip sig. of this field 
				}//for i	

				//} //if goodtuple
				if((bat!=0 && bat%100==0) || bat==size)
					for(int i=0;i<SigCNames.length;i++){
						int[] rows = stmt3[i].executeBatch();
						if(rows[0]<1) System.err.println("Failed to add Sig. row to table "+SigCNameTbl);
					}

			}// while

			stmt.close();
			stmt2.close();
			for(int i=0;i<SigCNames.length;i++)
				stmt3[i].close();
		} catch (SQLException e) {

			System.err.println("Failed to create Sig. Table "+SigCNameTbl);
			e.printStackTrace();
			return 0;
		} 

		return 1;
	}


	//=========================================================================
	//Create Phonetics  tables for certain fields (columns)
	public int CreatePhonTableMultiSig(Connection conn,String Tble, String[] SigCNames,  String[]  SigRecLinkFieldNames){
		String sqlAdd,sqlDrop,sql,sqlIns;
		//String[] sqlIns=new String[SigCNames.length];
		Statement stmt = null,stmt2=null;
		PreparedStatement stmt3[]=new PreparedStatement[SigCNames.length];
		String SigCNameTbl="NoNullPhn"+"_"+Tble;


		try {
			stmt = conn.createStatement();

			sql="SELECT * from "+ Tble;

			stmt2 = conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			//: Execute a query

			ResultSet rs = stmt2.executeQuery(sql);




			for(int i=0;i<SigCNames.length;i++){

				SigCNameTbl="NoNullPhn"+"_"+Tble+SigCNames[i];


				sqlDrop="Drop Table If exists "+SigCNameTbl+";";
				sqlAdd = "CREATE TABLE if not exists "+SigCNameTbl+" ("+
						"id SERIAL,"+
						"RecId int DEFAULT NULL,"+
						"SndxStr varchar(20) DEFAULT NULL,"+ 
						"DmPhStr1 varchar(20) DEFAULT NULL,"+ 
						"DmPhStr2 varchar(20) DEFAULT NULL,"+ 
						"NyStr varchar(20) DEFAULT NULL,"+ 

									"PRIMARY KEY (id)  "+

									") ;";

				stmt.executeUpdate(sqlDrop);
				stmt.executeUpdate(sqlAdd);

				String sqlDropndx,sqlAddndx;
				String[] Indexes={"RecId ","DmPhStr1,DmPhStr2","SndxStr ","NyStr"};
				for(int ndx=0;ndx<Indexes.length;ndx++){
					sqlDropndx="DROP INDEX if exists "+SigCNameTbl+"ndx"+ndx;
					sqlAddndx="CREATE INDEX "+SigCNameTbl+"ndx"+ndx+
							" ON " +SigCNameTbl+" USING btree ("+Indexes[ndx]+"); ";
					stmt.executeUpdate(sqlDropndx);
					stmt.executeUpdate(sqlAddndx);
				}
			}




			// Loop through result set and add Sig
			//Move to BFR position so while-loop works properly

			boolean goodTuple=true;
			int bat=0;
			rs.last();
			int size= rs.getRow();
			rs.beforeFirst();
			String[] Str1=new String[SigCNames.length];

			for(int i=0;i<SigCNames.length;i++){
				SigCNameTbl="NoNullPhn"+"_"+Tble+SigCNames[i];
				sqlIns="Insert into "+SigCNameTbl+" (RecId,SndxStr,DmPhStr1,DmPhStr2,NyStr) Values (?,?,?,?,?) ;";
				stmt3[i]=conn.prepareStatement(sqlIns);
			}

			while(rs.next()){
				goodTuple=true;
				bat++;
				int RecId=rs.getInt(SigRecLinkFieldNames[0]);

				//Retrieve by column name
				for(int i=0;i<SigCNames.length;i++){
					Str1[i]= rs.getString(SigCNames[i]);
					if ( (Str1[i] ==null ) || (Str1[i].length()==0)) 
						goodTuple=false;
				}	
				if(goodTuple){

					for(int i=0;i<SigCNames.length;i++){
						Phonetics phstr1=new Phonetics(Str1[i]);

						stmt3[i].setInt(1, RecId);  // Link field as FK to org. table
						stmt3[i].setString(2, phstr1.SndxStr); //Sig Id is its index
						stmt3[i].setString(3, phstr1.DmPhStr1);
						stmt3[i].setString(4, phstr1.DmPhStr2);
						stmt3[i].setString(5, phstr1.NyStr);
						stmt3[i].addBatch();

					}//for i	

				} //if goodtuple
				if((bat!=0 && bat%100==0) || bat==size)
					for(int i=0;i<SigCNames.length;i++){
						int[] rows = stmt3[i].executeBatch();
						if(rows[0]<1) System.err.println("Failed to add phonetics row to table "+SigCNameTbl);
					}

			}// while

			stmt.close();
			stmt2.close();
			for(int i=0;i<SigCNames.length;i++)
				stmt3[i].close();
		} catch (SQLException e) {

			System.err.println("Failed to create phonetics Table "+SigCNameTbl);
			e.printStackTrace();
			return 0;
		} 

		return 1;
	}

	//=========================================================================



}//end class





