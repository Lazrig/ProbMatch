/*
 * This package for phonetics  probabilistic record matching, To compare with LSH
 * 
 * By: Ibrahim Lazrig, Lazrig@cs.colostate.edu or Lazrig@gmail.com
 */



package probmatch;

//import org.apache.commons.codec.*;
import org.apache.commons.codec.language.*;

public class Phonetics {
	public String SndxStr;
	public String DmPhStr1;
	public String DmPhStr2;
	public String NyStr;

	public Phonetics(String Str) {

		try{
			Soundex sndx = new Soundex();
			DoubleMetaphone doubleMetaphone = new DoubleMetaphone();
			Nysiis NysCode = new Nysiis(false);

			SndxStr = sndx.encode(Str);
			DmPhStr1 = doubleMetaphone.doubleMetaphone(Str);
			DmPhStr2 = doubleMetaphone.doubleMetaphone(Str,true);
			NyStr = NysCode.nysiis(Str);
		}catch(Exception e)
		{
			System.err.println(e);
		}
	}

	@Override
	public String toString() {
		return "Phonetics [SndxStr=" + SndxStr + ", DmPhStr1=" + DmPhStr1
				+ ", DmPhStr2=" + DmPhStr2 + ", NyStr=" + NyStr + "]";
	}


}
