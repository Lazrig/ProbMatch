/*
 * This Package to save and load  The client program Configurations 
 * 
 * By: Ibrahim Lazrig, Lazrig@cs.colostate.edu or Lazrig@gmail.com
 */


package clientconfig;

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
import javax.swing.Box;
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
	private File configFile = new File("ClientConfig.properties");
	private Properties configProps;
	public boolean alreadyDisposed = false;
	
	
	
	private JPanel contentPane;
	private JTextField textDB1url;
	
	
	private JTextField textTable1;
	private JTextField textUser1;
	private JTextField textPass1;
	
	private JTextField textLinkageColumnsToMakeSigs;
	private JTextField textSigsToRecsLink;
	private JTextField textnHashes;
	private JTextField textSigSimilarityTreshold;
	private String SimTRforEachColumnAsString;
	private JCheckBox chckbxUseExtBiGrams;

	
	public SysConfigMainWindow( final Config CurrentConfig) {
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE );
		setBounds(100, 100, 741, 505);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JLabel lblFirstDataSource = new JLabel("Data Source");
		lblFirstDataSource.setBounds(30, 32, 123, 15);
		contentPane.add(lblFirstDataSource);
		
		
		
		textDB1url = new JTextField();
		textDB1url.setBounds(40, 70, 297, 19);
		contentPane.add(textDB1url);
		textDB1url.setColumns(10);
		
		
		
		
		
		JLabel lblTableName = new JLabel("Table name");
		lblTableName.setBounds(569, 32, 123, 15);
		contentPane.add(lblTableName);
		
		textTable1 = new JTextField();
		textTable1.setColumns(10);
		textTable1.setBounds(569, 70, 114, 19);
		contentPane.add(textTable1);
		
		JLabel lblUserName = new JLabel("User Name");
		lblUserName.setBounds(44, 176, 109, 17);
		contentPane.add(lblUserName);
		
		JLabel lblPassword = new JLabel("PassWord");
		lblPassword.setBounds(424, 178, 136, 15);
		contentPane.add(lblPassword);
		
		textUser1 = new JTextField();
		textUser1.setBounds(123, 174, 114, 19);
		contentPane.add(textUser1);
		textUser1.setColumns(10);
		
		textPass1 = new JTextField();
		textPass1.setBounds(558, 174, 114, 19);
		contentPane.add(textPass1);
		textPass1.setColumns(10);
		
		
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
		
		
		defaultProps.setProperty( "DB_URL", "jdbc:postgresql://localhost/TestData");
		defaultProps.setProperty( "USER", "user1");
		defaultProps.setProperty( "PASS", "User1");
		
		
		
		defaultProps.setProperty( "Timesfilename", "TimingResults2.txt");
		defaultProps.setProperty( "Fpfilename","FpResults.txt");
		defaultProps.setProperty( "UseExtBiGrams", "true");
		
		//StartOver to create the SigTables again
		//boolean UsePhon=false,startOver=false;
		defaultProps.setProperty( "SigRecLink","id");
		defaultProps.setProperty( "SigCNames","firstname,lastname,cob,DateOB") ;
		defaultProps.setProperty( "SimTRforEachColumnAsString","0.5,0.5,0.5,0.5");
		defaultProps.setProperty( "TableName","Source2A");
		// we can set number of hases, and TR, then set these parameters based on them.
		
		// boolean DoPhase1=true;
		defaultProps.setProperty( "nHashes","50");
		defaultProps.setProperty( "SigMatchingTreshold","0.5");
		
		
		
		configProps = new Properties(defaultProps);
		
		// loads properties from file
		InputStream inputStream = new FileInputStream(configFile);
		configProps.loadFromXML(inputStream);
		inputStream.close();
		
		textDB1url.setText(configProps.getProperty( "DB_URL"));
		textUser1.setText(configProps.getProperty( "USER"));
		textPass1.setText(configProps.getProperty( "PASS") );
		
		chckbxUseExtBiGrams.setSelected(configProps.getProperty( "UseExtBiGrams").equals("true") );
		textSigsToRecsLink.setText(configProps.getProperty( "SigRecLink") );
		textLinkageColumnsToMakeSigs.setText(configProps.getProperty( "SigCNames")) ;
		textTable1.setText(configProps.getProperty( "TableName"));		
		
		textnHashes.setText(configProps.getProperty( "nHashes"));
		textSigSimilarityTreshold.setText(configProps.getProperty( "SigMatchingTreshold"));
		SimTRforEachColumnAsString=configProps.getProperty( "SimTRforEachColumnAsString");
		
		//SetCurrentConfigurations(CurrentConfig);
	}
	
	private void saveProperties(Config CurrentConfig) throws IOException {
		
		
		
		configProps.setProperty( "DB_URL", textDB1url.getText());
		configProps.setProperty( "USER", textUser1.getText());
		configProps.setProperty( "PASS", textPass1.getText());
	
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
		configProps.setProperty( "TableName",textTable1.getText());	
		
		// we can set number of hases, and TR, then set these parameters based on them.
		
		// boolean DoPhase1=true;
		configProps.setProperty( "nHashes",textnHashes.getText());
		// TR here for all columns, So we can make TR for each column
		configProps.setProperty( "SigMatchingTreshold",textSigSimilarityTreshold.getText());
		// Columns used during matching process to calculate the matching result based on minColsWeight4Match
		//for broker ...configProps.setProperty( "ColumnsUsed4TR",textColumnsUsed4TR.getText());
		//for broker ...configProps.setProperty( "minColsWeight4Match",textminColsWeight4Match.getText());
		
		
		
		OutputStream outputStream = new FileOutputStream(configFile);
		configProps.storeToXML(outputStream, "Client setttings");
		outputStream.close();
		SetCurrentConfigurations(CurrentConfig);
	}
	
	
	public boolean SetCurrentConfigurations(Config o){
		
		boolean ext=chckbxUseExtBiGrams.isSelected();
		o.DBurl = textDB1url.getText();
		o.InTable = textTable1.getText();
		o.User = textUser1.getText();
		o.Pass = textPass1.getText();
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
