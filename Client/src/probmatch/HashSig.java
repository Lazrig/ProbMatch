/*
 * This Package for the probabilistic record matching, to create LSH signatures
 * 
 * By: Ibrahim Lazrig, Lazrig@cs.colostate.edu or Lazrig@gmail.com
 */


package probmatch;



import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashSig {
	int nGroups=3;
	int nGitems=4;
	int hash_count=nGroups*nGitems;
	long size=10000;



	public HashSig(int nGroups, int nGitems) {
		super();
		this.nGroups = nGroups;
		this.nGitems = nGitems;
		this.hash_count =nGroups*nGitems;
		//this.size = size;
	}


	////////////////////////////////////////
	public long[] MinHash(String Str, int nGrams,boolean UseExtBiGrams){
		boolean verbose=false;
		long[] minh=new long[hash_count];
		String s;
		long result;
		HashFunction hf;
		List<String> x =StringTonGramsList(Str,2,UseExtBiGrams);  

		for( int seed=0; seed<hash_count; seed++){
			minh[seed]=Long.MAX_VALUE;
			hf= Hashing.murmur3_32(seed);

			for( int i=0; i<x.size();i++){
				s=x.get(i);

				// for( int i=0; i<Str.length()-1;i++){
				//  s=Str.substring(i, i+nGrams);
				HashCode hc=hf.hashUnencodedChars(s);
				if(verbose){
					System.out.print(s +" | ");
					System.err.println("Hash of "+s+" : " + hc.asInt()+" As string: "+hc.toString()+" to long :"+Long.decode("0x"+hc.toString()) );
				}
				// result = Long.decode("0x"+hc.toString())  % size;
				result = Long.decode("0x"+hc.toString())  & 0xffffffffL % size;
				if( result < minh[seed])
					minh[seed]=result;

			}
			if(verbose)
				System.out.println("MinHash of "+seed+" : " + minh[seed]);
		}

		return minh; 
	}


	////////////////////////////////////
	public long[]  GetSig(String Str, int nGrams,boolean UseExtBiGrams,boolean Lcase){
		boolean verbose=false;
		if(Lcase) Str=Str.toLowerCase();
		long[] minh=MinHash(Str, nGrams,UseExtBiGrams);
		if(verbose)
			System.out.println("MinHash in Sig: " +Arrays.toString(minh));
		HashCode hc;
		long[] Sig= new long[nGroups]; 
		String s;
		HashFunction hf=Hashing.murmur3_32(2000);

		for(int b=1, i=0; b<=nGroups; b++){
			s="";	
			for(;i<b*nGitems;i++)
				s=s+minh[i];
			if (verbose)
				System.out.println("minh cvrt to str: "+s);
			hc=hf.hashUnencodedChars(s);
			Sig[b-1]= Long.decode("0x"+hc.toString()) % 1000000;
			if (verbose)
				System.out.println("Sig["+b+"]="+Sig[b-1]);
		}

		return Sig;
	}
	//////////////////////////////////////

	List <String> StringTonGramsList(String Str,int  nGrams,boolean UseExtBiGrams){
		List<String> x=new  ArrayList<String>();
		String s;
		if (! UseExtBiGrams){
			String Str2='?'+Str+'?';
			for( int i=0; i<Str2.length()-1;i++){
				s=Str2.substring(i, i+nGrams);
				x.add(s);
			}
		} else{
			String Str2='?'+Str+'?';
			for( int i=0; i<Str2.length()-1;i++){
				s=Str2.substring(i, i+nGrams);
				x.add(s);
			}
			for(int i=0;i<Str.length()-2;i++){
				s=Str.substring(i,i+1)+Str.substring(i+2, i+3);
				x.add(s);
			}
			//extended biGrams
		}
		return x;
	}
	//////////////////////////////////////////////

}
