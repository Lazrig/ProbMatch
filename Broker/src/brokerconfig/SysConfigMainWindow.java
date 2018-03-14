/*
 * This Package for the Broker program to save and load its configurations
 * 
 * By: Ibrahim Lazrig, Lazrig@cs.colostate.edu or Lazrig@gmail.com
 */



package brokerconfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.Timer;
import javax.swing.JCheckBox;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.Font;

public class SysConfigMainWindow extends JFrame{// implements WindowListener {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1955636229006053678L;
	private File configFile = new File("BrokerConfig.properties");
	private Properties configProps;
	public boolean alreadyDisposed = false;
	
	
	
	private JPanel contentPane;
	private JTextField textSRC1DBurl;
	
	
	private JTextField textSRC2Table;
	private JTextField textSRC1Table;
	
	private JTextField textSRC1Pass;
	private JTextField textSRC1User;
	//private JTextField textSRC2Pass;
	//private JTextField textSRC2User;
	
	private JTextField textLinkageColumnsToMakeSigs;
	private JTextField textSigsToRecsLink;
	private JTextField textnHashes;
	private JTextField textSigSimilarityTreshold;
	private String SimTRforEachColumnAsString;
	private JCheckBox chckbxUseExtBiGrams;
	private JLabel lblDataSource2;
	//private JTextField textSRC2DBurl;
	
	private JLabel lblSrc1Table;
	//private JLabel lblSrc2Uname;
	
	private JLabel lblSrc1Password;

		public SysConfigMainWindow( final Config CurrentConfig) {
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE );
		setBounds(100, 100, 741, 505);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JLabel lblFirstDataSource = new JLabel("Data Source 1");
		lblFirstDataSource.setBounds(30, 32, 123, 15);
		contentPane.add(lblFirstDataSource);
		
		
		
		textSRC1DBurl = new JTextField();
		textSRC1DBurl.setBounds(40, 70, 297, 19);
		contentPane.add(textSRC1DBurl);
		textSRC1DBurl.setColumns(10);
		
		
		
		
		
		JLabel lblSrc2TableName = new JLabel("Table name");
		lblSrc2TableName.setBounds(412, 111, 123, 15);
		contentPane.add(lblSrc2TableName);
		
		textSRC2Table = new JTextField();
		textSRC2Table.setColumns(10);
		textSRC2Table.setBounds(532, 109, 114, 19);
		contentPane.add(textSRC2Table);
		
		JLabel lblSrc1UName = new JLabel("User Name");
		lblSrc1UName.setBounds(44, 153, 109, 17);
		contentPane.add(lblSrc1UName);
		
		/*JLabel lblSrc2Password = new JLabel("PassWord");
		lblSrc2Password.setBounds(424, 178, 136, 15);
		contentPane.add(lblSrc2Password);
		*/
		textSRC1User = new JTextField();
		textSRC1User.setBounds(159, 152, 114, 19);
		contentPane.add(textSRC1User);
		textSRC1User.setColumns(10);
		
		/*
		textSRC2Pass = new JTextField();
		textSRC2Pass.setBounds(511, 176, 114, 19);
		contentPane.add(textSRC2Pass);
		textSRC2Pass.setColumns(10);
		*/
		
		lblDataSource2 = new JLabel("Data Source 2");
		lblDataSource2.setBounds(392, 32, 123, 15);
		contentPane.add(lblDataSource2);
		
		/* textSRC2DBurl = new JTextField();
		textSRC2DBurl.setText((String) null);
		textSRC2DBurl.setColumns(10);
		textSRC2DBurl.setBounds(402, 70, 297, 19);
		contentPane.add(textSRC2DBurl); */
		
		textSRC1Table = new JTextField();
		textSRC1Table.setText((String) null);
		textSRC1Table.setColumns(10);
		textSRC1Table.setBounds(170, 107, 114, 19);
		contentPane.add(textSRC1Table);
		
		lblSrc1Table = new JLabel("Table name");
		lblSrc1Table.setBounds(50, 109, 123, 15);
		contentPane.add(lblSrc1Table);
		/*
		lblSrc2Uname = new JLabel("User Name");
		lblSrc2Uname.setBounds(412, 153, 109, 17);
		contentPane.add(lblSrc2Uname);
		
		textSRC2User = new JTextField();
		textSRC2User.setColumns(10);
		textSRC2User.setBounds(491, 151, 114, 19);
		contentPane.add(textSRC2User);
		*/
		textSRC1Pass = new JTextField();
		textSRC1Pass.setColumns(10);
		textSRC1Pass.setBounds(169, 182, 114, 19);
		contentPane.add(textSRC1Pass);
		
		lblSrc1Password = new JLabel("PassWord");
		lblSrc1Password.setBounds(72, 184, 136, 15);
		contentPane.add(lblSrc1Password);
		
		JLabel lblLinkageColumnNames = new JLabel("Linkage Column Names");
		lblLinkageColumnNames.setBounds(44, 291, 193, 15);
		contentPane.add(lblLinkageColumnNames);
		
		textLinkageColumnsToMakeSigs = new JTextField();
		textLinkageColumnsToMakeSigs.setBounds(232, 289, 349, 19);
		contentPane.add(textLinkageColumnsToMakeSigs);
		textLinkageColumnsToMakeSigs.setColumns(10);
		
		JLabel lblLinkRecordid = new JLabel("Link Record-id");
		lblLinkRecordid.setBounds(50, 318, 103, 15);
		contentPane.add(lblLinkRecordid);
		
		textSigsToRecsLink = new JTextField();
		textSigsToRecsLink.setBounds(189, 318, 114, 19);
		contentPane.add(textSigsToRecsLink);
		textSigsToRecsLink.setColumns(10);
		
		textnHashes = new JTextField();
		textnHashes.setBounds(189, 349, 114, 19);
		contentPane.add(textnHashes);
		textnHashes.setColumns(10);
		
		JLabel lblHashes = new JLabel("# Hashes");
		lblHashes.setBounds(50, 353, 70, 15);
		contentPane.add(lblHashes);
		
		JLabel lblSimilarityTr = new JLabel("Similarity TR");
		lblSimilarityTr.setBounds(342, 351, 136, 15);
		contentPane.add(lblSimilarityTr);
		
		textSigSimilarityTreshold = new JTextField();
		textSigSimilarityTreshold.setBounds(451, 349, 114, 19);
		contentPane.add(textSigSimilarityTreshold);
		textSigSimilarityTreshold.setColumns(10);
		
		chckbxUseExtBiGrams = new JCheckBox("UseExtBiGrams");
		chckbxUseExtBiGrams.setBounds(392, 419, 203, 23);
		contentPane.add(chckbxUseExtBiGrams);
		
		JButton buttonSave = new JButton("Save");
		buttonSave.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				
				try {
					saveProperties(CurrentConfig);
					JOptionPane.showMessageDialog(SysConfigMainWindow.this, 
							"Properties were saved successfully!");	
					//SetCurrentConfigurations(CurrentConfig);
					alreadyDisposed = true;
					CurrentConfig.dataSaved=true;
					 dispose();
				} catch (IOException ex) {
					JOptionPane.showMessageDialog(SysConfigMainWindow.this, 
							"Error saving properties file: " + ex.getMessage());		
				}
				}	
			
		});
		buttonSave.setBounds(586, 447, 117, 25);
		contentPane.add(buttonSave);
		
		
		
		JButton btnSetTRforCols = new JButton("set TR for Columns");
		btnSetTRforCols.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				String[] Cols=textLinkageColumnsToMakeSigs.getText().split("\\s*,\\s*");
				JTextField[]  inputTxtFields=new JTextField[Cols.length];
				
				JPanel myPanel = new JPanel();
				String[] TRs=SimTRforEachColumnAsString.split("\\s*,\\s*");
				 
				for(int i=0;i<Cols.length;i++)
				{ inputTxtFields[i]=new JTextField(5);
				if(TRs[i].isEmpty())
					inputTxtFields[i].setText(textSigSimilarityTreshold.getText());//"0.5"); // default value
					
				else
					inputTxtFields[i].setText(TRs[i]);
				
				
				  myPanel.add(new JLabel(Cols[i]) );
			      myPanel.add(inputTxtFields[i]);
			     // myPanel.add(Box.createHorizontalStrut(15)); // a spacer
				}
				int result = JOptionPane.showConfirmDialog(null, myPanel, 
			               "Please Enter Similarity TR for each Column", JOptionPane.OK_CANCEL_OPTION);
			      if (result == JOptionPane.OK_OPTION) {
			    	  SimTRforEachColumnAsString="";
			    	  for(int i=0;i<Cols.length;i++){
			    		  if(i==0)
			    			  SimTRforEachColumnAsString= inputTxtFields[i].getText();
			    		  else
			    		     SimTRforEachColumnAsString=  SimTRforEachColumnAsString+","+inputTxtFields[i].getText();
			    		  
			         System.out.println("\n Sim TR for: " +Cols[i]+"="+ inputTxtFields[i].getText());
			    	  }
			         
			      }
				
				
			}
		});
		btnSetTRforCols.setFont(new Font("Dialog", Font.BOLD, 9));
		btnSetTRforCols.setBounds(593, 286, 136, 25);
		contentPane.add(btnSetTRforCols);
		
	/*	addWindowListener(new WindowAdapter() {
		    public void windowClosing(WindowEvent e) {
		        // Do what you want when the window is closing.
		    }
		});*/


	try {
		loadProperties(CurrentConfig);
	} catch (IOException ex) {
		JOptionPane.showMessageDialog(this, "The config.properties file does not exist, default properties loaded.");
	}
	
			
	

} // constructor
	

	
	 public static void createAndShowGUI(Config CurrentConfig)
	    {
		  SysConfigMainWindow frame = new SysConfigMainWindow(CurrentConfig);
			
	        
			frame.pack();
			frame.setVisible(true);
	    }
	
	
	
	
	private void loadProperties( Config CurrentConfig) throws IOException {
		Properties defaultProps = new Properties();
		// sets default properties
		
		defaultProps.setProperty( "Source1_DB1_URL",  "jdbc:postgresql://localhost/TestData");
		defaultProps.setProperty( "SRC1_USER","user1");
		defaultProps.setProperty( "SRC1_PASS","User1");
		defaultProps.setProperty( "Source_1_TableName","Source2A");
		defaultProps.setProperty( "Source_2_TableName","Source2B");	

		defaultProps.setProperty( "Timesfilename", "BrokerTimingResults.txt");
		defaultProps.setProperty( "Fpfilename","BrokerFpResults.txt");
		defaultProps.setProperty( "UseExtBiGrams", "true");
		
		//StartOver to create the SigTables again
		//boolean UsePhon=false,startOver=false;
		defaultProps.setProperty( "SigRecLink","id");
		defaultProps.setProperty( "SigCNames","firstname,lastname,cob,DateOB") ;
		defaultProps.setProperty( "SimTRforEachColumnAsString","0.5,0.5,0.5,0.5");
		
		// we can set number of hahses, and TR, then set these parameters based on them.
		
		// boolean DoPhase1=true;
		defaultProps.setProperty( "nHashes","50");
		defaultProps.setProperty( "SigMatchingTreshold","0.5");
				
		
		
		
		
		configProps = new Properties(defaultProps);
		
		// loads properties from file
		InputStream inputStream = new FileInputStream(configFile);
		configProps.loadFromXML(inputStream);
		inputStream.close();
		

		
		textSRC1DBurl.setText(configProps.getProperty( "Source1_DB1_URL"));
		String tt=configProps.getProperty( "SRC1_USER");
		textSRC1User.setText(tt);
		tt=configProps.getProperty( "SRC1_PASS");
		textSRC1Pass.setText(tt);
		
		textSRC1Table.setText(configProps.getProperty( "Source_1_TableName"));	
		
		textSRC2Table.setText(configProps.getProperty( "Source_2_TableName"));

		/*
		textSRC2DBurl.setText(configProps.getProperty( "Source2_DB1_URL"));
		
		textSRC2User.setText(configProps.getProperty( "SRC2_USER"));
		
		textSRC2Pass.setText(configProps.getProperty( "SRC2_PASS"));
		*/
		
		
		
				
				chckbxUseExtBiGrams.setSelected(configProps.getProperty( "UseExtBiGrams").equals("true") );
				textSigsToRecsLink.setText(configProps.getProperty( "SigRecLink") );
				textLinkageColumnsToMakeSigs.setText(configProps.getProperty( "SigCNames")) ;
				//textSRC2Table.setText(configProps.getProperty( "TableName"));		
				
				textnHashes.setText(configProps.getProperty( "nHashes"));
				textSigSimilarityTreshold.setText(configProps.getProperty( "SigMatchingTreshold"));
				SimTRforEachColumnAsString=configProps.getProperty( "SimTRforEachColumnAsString");
				
		
		
		//SetCurrentConfigurations(CurrentConfig);
		
		
		
	}
	
	private void saveProperties(Config CurrentConfig) throws IOException {
		
		
		
		configProps.setProperty( "Source1_DB1_URL", textSRC1DBurl.getText());
		configProps.setProperty( "SRC1_USER", textSRC1User.getText());
		configProps.setProperty( "SRC1_PASS", textSRC1Pass.getText());
		configProps.setProperty( "Source_1_TableName",textSRC1Table.getText());

		/*configProps.setProperty( "Source2_DB1_URL", textSRC2DBurl.getText());
		configProps.setProperty( "SRC2_USER", textSRC2User.getText());
		configProps.setProperty( "SRC2_PASS", textSRC2Pass.getText());
		*/
		configProps.setProperty( "Source_2_TableName",textSRC2Table.getText());	
		
		
		String ExtBiG=(chckbxUseExtBiGrams.isSelected())? "true":"false";
		configProps.setProperty( "UseExtBiGrams",ExtBiG );
		
		
		//configProps.setProperty( "Timesfilename", textTimingResultsFile.getText());
		//configProps.setProperty( "Fpfilename",textAccuracyResultsFile.getText());
		
		//Lcase=false;
		//boolean newSession=false; 
		//boolean newSession=true;
		// newSession For doing the matching processes again, if not then just compute Fp,Fn,Tp
		//StartOver to create the SigTables again
		//boolean UsePhon=false,startOver=false;
		configProps.setProperty( "SigRecLink", textSigsToRecsLink.getText() );
		configProps.setProperty( "SigCNames",textLinkageColumnsToMakeSigs.getText()) ;
		configProps.setProperty( "SimTRforEachColumnAsString",SimTRforEachColumnAsString);
		
		
		// we can set number of hases, and TR, then set these parameters based on them.
	
		// boolean DoPhase1=true;
		configProps.setProperty( "nHashes",textnHashes.getText());
		// TR here for all columns, So we can make TR for each column
		configProps.setProperty( "SigMatchingTreshold",textSigSimilarityTreshold.getText());
				
		
		
		OutputStream outputStream = new FileOutputStream(configFile);
		configProps.storeToXML(outputStream, "Broker setttings");
		outputStream.close();
		SetCurrentConfigurations(CurrentConfig);
	}
	
	
	public boolean SetCurrentConfigurations(Config o){
		
		boolean ext=chckbxUseExtBiGrams.isSelected();
		 o.Source1DBurl = textSRC1DBurl.getText();
		// o.Source2DBurl = textSRC2DBurl.getText();
		 o.InTablesSource1 = textSRC1Table.getText();
		 o.InTablesSource2 = textSRC2Table.getText();
		 o.Source1User = textSRC1User.getText();
		 o.Src1UserPass = textSRC1Pass.getText();
		 //o.Source2User = textSRC2User.getText();
		 //o.Src2UserPass = textSRC2Pass.getText();
		 
		
		o.LinkageColumnsToMakeSigs = textLinkageColumnsToMakeSigs.getText().split("\\s*,\\s*");
		o.LinkColsOfSigsToRecs = textSigsToRecsLink.getText().split("\\s*,\\s*");
		o.nHashes = Integer.valueOf(textnHashes.getText());
		o.SigSimilarityTreshold = Double.valueOf(textSigSimilarityTreshold.getText());
		
		o.SimTRforEachColumn=new double[o.LinkageColumnsToMakeSigs.length];
		String[] TRs=SimTRforEachColumnAsString.split("\\s*,\\s*");
		for(int i=0;i<o.LinkageColumnsToMakeSigs.length;i++){
			if(TRs[i].isEmpty())
				o.SimTRforEachColumn[i]=o.SigSimilarityTreshold ;
			else
				o.SimTRforEachColumn[i]=Double.valueOf(TRs[i]);
		}
		//for broker ...o.ColumnsUsed4CalculatingMatchTR = textColumnsUsed4TR.getText().split("\\s*,\\s*");
		//for broker ...o.minTotColsWeight4Match = Double.valueOf(textminColsWeight4Match.getText());
		o.UseExtBiGrams = (ext)? true:false;
		
		
	return true;	
	}
}
