package brokerconfig;

public class Config {
	
	
	public String Source1DBurl;
	//public String Source2DBurl;
	public String InTablesSource1;
	public String InTablesSource2;
	
	public String Source1User;
	public String Src1UserPass;
	//public String Source2User;
	//public String Src2UserPass;
	
	public String[] LinkageColumnsToMakeSigs; // used here to create tables names
	public String[] LinkColsOfSigsToRecs;
	public int nHashes;
	public double SigSimilarityTreshold;
	public double[] SimTRforEachColumn;
	public int nGroups;
	public int nGitems;
	public boolean UseExtBiGrams;
	public boolean UsePhon;
	public boolean newSession=true;
	public boolean DoPhase1=true;
	// above properties just needed to create tables names
	
	
	public String[] ColumnsUsed4CalculatingMatchTR;
	public double minTotColsWeight4Match;
	
	public boolean dataSaved;
	
	
	public Config() {
		super();
		
		
		
		dataSaved=false;
		 Source1DBurl = "jdbc:postgresql://localhost/TestData";
		 //Source2DBurl = "jdbc:postgresql://localhost/TestData";
		 InTablesSource1 = "Source2A";
		 InTablesSource2 = "Source2B";
		 Source1User = "user1";
		 Src1UserPass = "User1";
		 //Source2User = "user1";
		 //Src2UserPass = "User1";
		LinkageColumnsToMakeSigs =new String[] {"firstname","lastname","cob","DateOB"};
		LinkColsOfSigsToRecs = new String []{"id"};
		nHashes = 50;
		nGroups=10;
		nGitems=5;
		SigSimilarityTreshold = 0.5;
		SimTRforEachColumn=new double[] {0.5,0.5,0.5,0.5};
		ColumnsUsed4CalculatingMatchTR = new String[] {"firstname","lastname","cob","DateOB"};
		minTotColsWeight4Match = 2;
		UseExtBiGrams = true;
		UsePhon=false;
		newSession=true;
		DoPhase1=true;
		
	}


	
	
}
