package clientconfig;

public class Config {
	
	
	public String DBurl;
	
	public String InTable;
	
	public String User;
	public String Pass;
	
	public String[] LinkageColumnsToMakeSigs;
	public String[] LinkColsOfSigsToRecs;
	public int nHashes;
	public int nGroups;
	public int nGitems;
	public double SigSimilarityTreshold;
	public String[] ColumnsUsed4CalculatingMatchTR;
	//public double minTotColsWeight4Match;
	public boolean UseExtBiGrams;
	public boolean UsePhon;
	public boolean dataSaved;

	public double[] SimTRforEachColumn;
	
	
	public Config() {
		super();
		
		
		
		dataSaved=false;
		DBurl = "jdbc:postgresql://localhost/TestData";
		InTable = "Source2A";
		User = "user1";
		Pass = "User1";
		LinkageColumnsToMakeSigs =new String[] {"firstname","lastname","cob","DateOB"};
		LinkColsOfSigsToRecs = new String []{"id"};
		nHashes = 50;
		nGroups=10;
		nGitems=5;
		SigSimilarityTreshold = 0.5;
		SimTRforEachColumn=new double[] {0.5,0.5,0.5,0.5};
		ColumnsUsed4CalculatingMatchTR = new String[] {"firstname","lastname","cob","DateOB"};
		//minTotColsWeight4Match = 2;
		UseExtBiGrams = true;
		UsePhon=false;
		
	}


	
	
}
