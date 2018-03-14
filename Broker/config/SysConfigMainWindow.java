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
import javax.swing.JCheckBox;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

public class SysConfigMainWindow extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1955636229006053678L;
	private File configFile = new File("ClientConfig.properties");
	private Properties configProps;
	
	
	
	
	private JPanel contentPane;
	private JTextField textDB1url;
	
	
	private JTextField textTable1;
	private JTextField textUser1;
	private JTextField textPass1;
	
	private JTextField textLinkageColumnsToMakeSigs;
	private JTextField textSigsToRecsLink;
	private JTextField textnHashes;
	private JTextField textSigSimilarityTreshold;
	private JTextField textColumnsUsed4TR;
	private JTextField textminColsWeight4Match;
	private JCheckBox chckbxUseExtBiGrams;

	/**
	 * Launch the application.
	 */
	//public static void main(String[] args) {
	/*public void SetConfiguration(final Config CurrentConfig) {
		EventQueue.invokeLater(new Runnable() {
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

	/**
	 * Create the frame.
	 */
	public SysConfigMainWindow(final Config CurrentConfig) {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
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
		lblTableName.setBounds(50, 105, 123, 15);
		contentPane.add(lblTableName);
		
		textTable1 = new JTextField();
		textTable1.setColumns(10);
		textTable1.setBounds(40, 132, 114, 19);
		contentPane.add(textTable1);
		
		JLabel lblUserName = new JLabel("User Name");
		lblUserName.setBounds(44, 176, 70, 15);
		contentPane.add(lblUserName);
		
		JLabel lblPassword = new JLabel("PassWord");
		lblPassword.setBounds(44, 217, 70, 15);
		contentPane.add(lblPassword);
		
		textUser1 = new JTextField();
		textUser1.setBounds(123, 174, 114, 19);
		contentPane.add(textUser1);
		textUser1.setColumns(10);
		
		textPass1 = new JTextField();
		textPass1.setBounds(123, 215, 114, 19);
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
		
		textColumnsUsed4TR = new JTextField();
		textColumnsUsed4TR.setBounds(267, 382, 310, 19);
		contentPane.add(textColumnsUsed4TR);
		textColumnsUsed4TR.setColumns(10);
		
		JLabel lblColumnsUsedFor = new JLabel("Columns Used for Matching");
		lblColumnsUsedFor.setBounds(50, 384, 220, 15);
		contentPane.add(lblColumnsUsedFor);
		
		JLabel lblMinColsWeight = new JLabel("min Cols Weight 4Match");
		lblMinColsWeight.setBounds(50, 423, 187, 15);
		contentPane.add(lblMinColsWeight);
		
		textminColsWeight4Match = new JTextField();
		textminColsWeight4Match.setBounds(223, 421, 114, 19);
		contentPane.add(textminColsWeight4Match);
		textminColsWeight4Match.setColumns(10);
		
		JCheckBox chckbxUseExtBiGrams = new JCheckBox("UseExtBiGrams");
		chckbxUseExtBiGrams.setBounds(392, 419, 203, 23);
		contentPane.add(chckbxUseExtBiGrams);
		
		JButton buttonSave = new JButton("Save");
		buttonSave.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				
				try {
					saveProperties(CurrentConfig);
					JOptionPane.showMessageDialog(SysConfigMainWindow.this, 
							"Properties were saved successfully!");	
					 
					 dispose();
				} catch (IOException ex) {
					JOptionPane.showMessageDialog(SysConfigMainWindow.this, 
							"Error saving properties file: " + ex.getMessage());		
				}
			}
		});
		buttonSave.setBounds(586, 447, 117, 25);
		contentPane.add(buttonSave);
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
	
	
			textDB1url.setText(configProps.getProperty( "DB_URL"));
			textUser1.setText(configProps.getProperty( "USER"));
			textPass1.setText(configProps.getProperty( "PASS") );
			
			chckbxUseExtBiGrams.setSelected(configProps.getProperty( "UseExtBiGrams").equals("true") );
			textSigsToRecsLink.setText(configProps.getProperty( "SigRecLink") );
			textLinkageColumnsToMakeSigs.setText(configProps.getProperty( "SigCNames")) ;
			textTable1.setText(configProps.getProperty( "Table"));		
			
			textnHashes.setText(configProps.getProperty( "nHashes"));
			textSigSimilarityTreshold.setText(configProps.getProperty( "SigMatchingTreshold"));
			//for broker ...textColumnsUsed4TR.setText(configProps.getProperty( "ColumnsUsed4TR"));
			//for broker ...textminColsWeight4Match.setText(configProps.getProperty( "minColsWeight4Match"));
	

} // constructor
	
	
	
	private void loadProperties(final Config CurrentConfig) throws IOException {
		Properties defaultProps = new Properties();
		// sets default properties
		
		
		defaultProps.setProperty( "DB_URL", "jdbc:postgresql://localhost/TestData");
		defaultProps.setProperty( "USER", "user1");
		defaultProps.setProperty( "PASS", "User1");
		
		
		
		defaultProps.setProperty( "Timesfilename", "TimingResults2.txt");
		defaultProps.setProperty( "Fpfilename","FpResults.txt");
		defaultProps.setProperty( "UseExtBiGrams", "true");
		//Lcase=false;
		//boolean newSession=false; 
		//boolean newSession=true;
		// newSession For doing the matching processes again, if not then just compute Fp,Fn,Tp
		//StartOver to create the SigTables again
		//boolean UsePhon=false,startOver=false;
		defaultProps.setProperty( "SigRecLink","id");
		defaultProps.setProperty( "SigCNames","firstname,lastname,cob,DateOB") ;
		
		defaultProps.setProperty( "TableName","Source2A");
		// we can set number of hases, and TR, then set these parameters based on them.
		//int[] nGroups={16,12,10,8,7,6,5},nGItems={3,4,5,6,7,8,10};
		// boolean DoPhase1=true;
		defaultProps.setProperty( "nHashes","50");
		defaultProps.setProperty( "SigMatchingTreshold","0.5");
		//for broker ...defaultProps.setProperty( "ColumnsUsed4TR","firstname,lastname,cob,DateOB");
		//for broker ...defaultProps.setProperty( "minColsWeight4Match","2");
		
		
		
		
		
		configProps = new Properties(defaultProps);
		
		// loads properties from file
		InputStream inputStream = new FileInputStream(configFile);
		configProps.loadFromXML(inputStream);
		inputStream.close();
		SetCurrentConfigurations(CurrentConfig);
	}
	
	private void saveProperties(final Config CurrentConfig) throws IOException {
		
		
		
		configProps.setProperty( "DB1_URL", textDB1url.getText());
		configProps.setProperty( "USER1", textUser1.getText());
		configProps.setProperty( "PASS1", textPass1.getText());
	
		
	
		//configProps.setProperty( "Timesfilename", textTimingResultsFile.getText());
		//configProps.setProperty( "Fpfilename",textAccuracyResultsFile.getText());
		configProps.setProperty( "UseExtBiGrams", (chckbxUseExtBiGrams.isSelected())? "true":"false");
		//Lcase=false;
		//boolean newSession=false; 
		//boolean newSession=true;
		// newSession For doing the matching processes again, if not then just compute Fp,Fn,Tp
		//StartOver to create the SigTables again
		//boolean UsePhon=false,startOver=false;
		configProps.setProperty( "SigRecLink", textSigsToRecsLink.getText() );
		configProps.setProperty( "SigCNames",textLinkageColumnsToMakeSigs.getText()) ;
		
		configProps.setProperty( "TableName",textTable1.getText());	
		
		// we can set number of hases, and TR, then set these parameters based on them.
		//int[] nGroups={16,12,10,8,7,6,5},nGItems={3,4,5,6,7,8,10};
		// boolean DoPhase1=true;
		configProps.setProperty( "nHashes",textnHashes.getText());
		// TR here for all columns, So we can make TR for each column
		configProps.setProperty( "SigMatchingTreshold",textSigSimilarityTreshold.getText());
		// Columns used during matching process to calculate the matching result based on minColsWeight4Match
		//for broker ...configProps.setProperty( "ColumnsUsed4TR",textColumnsUsed4TR.getText());
		//for broker ...configProps.setProperty( "minColsWeight4Match",textminColsWeight4Match.getText());
		
		
		
		OutputStream outputStream = new FileOutputStream(configFile);
		configProps.store(outputStream, "Client setttings");
		outputStream.close();
		SetCurrentConfigurations(CurrentConfig);
	}
	
	
	public boolean SetCurrentConfigurations(Config o){
		
		
		o.DBurl = textDB1url.getText();
		o.InTable = textTable1.getText();
		o.User = textUser1.getText();
		o.Pass = textPass1.getText();
		o.LinkageColumnsToMakeSigs = textLinkageColumnsToMakeSigs.getText().split("\\s*,\\s*");
		o.LinkColsOfSigsToRecs = textSigsToRecsLink.getText().split("\\s*,\\s*");
		o.nHashes = Integer.valueOf(textnHashes.getText());
		o.SigSimilarityTreshold = Double.valueOf(textSigSimilarityTreshold.getText());
		//for broker ...o.ColumnsUsed4CalculatingMatchTR = textColumnsUsed4TR.getText().split("\\s*,\\s*");
		//for broker ...o.minTotColsWeight4Match = Double.valueOf(textminColsWeight4Match.getText());
		o.UseExtBiGrams = (chckbxUseExtBiGrams.isSelected())? true:false;
		
		
	return true;	
	}



	
	
}
