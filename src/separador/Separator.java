package separador;

import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import java.time.LocalDate;
import static java.time.DayOfWeek.MONDAY;
import static java.time.DayOfWeek.SUNDAY;
import static java.time.temporal.TemporalAdjusters.nextOrSame;
import static java.time.temporal.TemporalAdjusters.previousOrSame;

public class Separator {
	private final static String READ_PATH = "src/data/csse_covid_19_data/csse_covid_19_daily_reports";
	private final static String WRITE_PATH = "src/dataResult";
	private final static String ISO = "8859_1";

	private final static String READ_CSV_SPLITER = ",";
	private final static String CSV_SPLITER = ";";

	private final static String HEADER = 
			"FIPS"  + CSV_SPLITER + "Admin2" + CSV_SPLITER 
			+ "Province_State" + CSV_SPLITER + "Country_Region" + CSV_SPLITER
			+ "Day" + CSV_SPLITER + "Start_Week" + CSV_SPLITER
			+ "And_Week" + CSV_SPLITER + "Start_Month" + CSV_SPLITER 
			+ "And_Month" + CSV_SPLITER	+ "Lat" + CSV_SPLITER
			+ "Long_" + CSV_SPLITER + "Confirmed" + CSV_SPLITER
			+ "Deaths" + CSV_SPLITER + "Recovered" + CSV_SPLITER
			+ "Active" + CSV_SPLITER + "Combined_Key" + CSV_SPLITER
			+ "\n";

	private final static SimpleDateFormat YYYY_MM_DD_T_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"); 
	private final static SimpleDateFormat YYYY_MM_DD_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	private final static SimpleDateFormat M_DD_YYYY_HH_MM = new SimpleDateFormat("M/dd/yyyy HH:mm");
	private final static SimpleDateFormat M_DD_YY_HH_MM = new SimpleDateFormat("M/dd/yy HH:mm");
	private final static SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd"); 
	private final static SimpleDateFormat sdfFinal = new SimpleDateFormat("dd/MM/yyyy");
	
	public static void main(String[] args) throws IOException, ParseException {
		List<List<List<String>>> allFiles = new ArrayList<List<List<String>>>();
		File folder = new File(READ_PATH);

		File[] listOfFiles = folder.listFiles();

		for (File file: listOfFiles) {
			if (file.isFile() && file.getName().contains(".csv")) {
				List<List<String>> fileRows = trasform(file);
				allFiles.add(fileRows);
				writeCSV(file.getName(), fileRows);
			}
		}
		
		writeGlobalFile(allFiles);
	}

	private static List<List<String>> trasform(File file) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), ISO));
		List<List<String>> csvTable = new ArrayList<>();
		List<String> row = new ArrayList<>();
		
		String tableHeader = bufferedReader.readLine().replaceAll("﻿", "");
		
		// Columns that may not exist 
		boolean hasFIPS = tableHeader.toUpperCase().contains("FIPS" + READ_CSV_SPLITER);
		boolean hasAdmin2 = tableHeader.toUpperCase().contains("ADMIN2" + READ_CSV_SPLITER);
		boolean hasLatitude = tableHeader.toUpperCase().contains("LATITUDE" + READ_CSV_SPLITER);
		boolean hasLongitude = tableHeader.toUpperCase().contains("LONGITUDE" + READ_CSV_SPLITER);
		boolean hasLat = tableHeader.toUpperCase().contains("LAT" + READ_CSV_SPLITER);
		boolean hasLong_ = tableHeader.toUpperCase().contains("LONG_" + READ_CSV_SPLITER);
		boolean hasActive = tableHeader.toUpperCase().contains("ACTIVE" + READ_CSV_SPLITER);
		boolean hasCombinedKey = tableHeader.toUpperCase().contains("COMBINED_KEY");
		
		int pps = hasFIPS ? 2 : 0; // PositionProvinceState: position at array of provice/state
		int pl = hasLatitude ? 6 : 5; // PositionLatitude: position at array of latitude/lat
		int pc = hasLat ? 7 : 3;  
		
		String line = bufferedReader.readLine();
		while (line != null) {
			List<String> cols = new ArrayList<>();
			boolean wrongLine = line.toString().contains("\"");
			
			for (String col : 
					wrongLine
					?
					line.toString()
					.toUpperCase()
					.replaceAll(", ", "/t")
					.replaceAll(",", "; ")
					.replaceAll("/t", ", ")
					.replaceAll("\"", "")
					.split(CSV_SPLITER)
					:
					line.toString()
					.toUpperCase()
					.replaceAll(",", "; ")
					.split(CSV_SPLITER)
			) {
				cols.add(col.startsWith(" ") ? col.replaceFirst(" ", "") : col);
			}
			
			
			row = new ArrayList<>(); 

			// FIPS
			row.add(hasFIPS && !cols.get(0).isEmpty() ? cols.get(0) : "0");

			// Admin2
			row.add(hasAdmin2 && !cols.get(1).isEmpty() ? cols.get(1) : "UNASSIGNED");

			// Province_State
			row.add(cols.get(pps).isEmpty() ? "UNDEFINED" : cols.get(pps));

			// Country_Region
			row.add(cols.get(pps + 1).isEmpty() ? "UNDEFINED" : cols.get(pps + 1));

			// Last_Update	
			String date = cols.get(pps + 2);
			String formattedDate = "";
			
			try {
				formattedDate = sdfFinal.format(YYYY_MM_DD_T_HH_MM_SS.parse(date));
			} catch (ParseException e1) {
				try {
					formattedDate = sdfFinal.format(YYYY_MM_DD_HH_MM_SS.parse(date));
				} catch (ParseException e2) {
					try {
						formattedDate = sdfFinal.format(M_DD_YY_HH_MM.parse(date));
					} catch (ParseException e3) {
						try {
							formattedDate = sdfFinal.format(M_DD_YYYY_HH_MM.parse(date));					
						} catch (ParseException e4) {
							e4.printStackTrace();
						}
					}
				}
			}
			row.add(formattedDate);

			try {
				Calendar calendar = Calendar.getInstance();  
				calendar.setTime(sdfFinal.parse(formattedDate));
				String [] s = formattedDate.split("/");
				LocalDate day = LocalDate.parse(s[2] + "-" + s[1] + "-" + s[0]);
				
				row.add(sdfFinal.format(sdf4.parse(day.with(previousOrSame(MONDAY)).toString())));
				row.add(sdfFinal.format(sdf4.parse(day.with(nextOrSame(SUNDAY)).toString())));
				row.add( "01" + "/" + s[1] + "/" + s[2]);
				row.add(calendar.getActualMaximum(Calendar.DAY_OF_MONTH) + "/" + s[1] + "/" + s[2]);
			} catch (ParseException e) {
				e.printStackTrace();
			}  			
	        
			// Lat
			row.add(hasLat || hasLatitude ? cols.get(pl) : "0");

			// Long_
			row.add(hasLong_ || hasLongitude ? cols.get(pl + 1) : "0");

			// Confirmed
			row.add(cols.get(pc).isEmpty() ? "0" : cols.get(pc));

			// Deaths
			row.add(cols.get(pc + 1).isEmpty() ? "0" : cols.get(pc + 1));

			// Recovered
			row.add(cols.get(pc + 2).isEmpty() ? "0" : cols.get(pc + 2));

			// Active
			row.add(hasActive && !cols.get(pc + 3).isEmpty() ? cols.get(pc + 3) : "0");

			// Combined_Key
			row.add(hasCombinedKey ? cols.get(pc + 4) : "UNDEFINED");

			csvTable.add(row);
			line = bufferedReader.readLine();
		}
		
		bufferedReader.close();
		return csvTable;
	}
	
	private static void writeCSV(String fileName, List<List<String>> rows) throws IOException {
		FileWriter fileWriter = new FileWriter(WRITE_PATH + "/" + fileName);
		fileWriter.append(HEADER);

		for (List<String> row : rows) {
			StringBuffer line = new StringBuffer();
			for(String column: row) {
				line.append(column + CSV_SPLITER);
			}
			line.deleteCharAt(line.length() - 1);
			fileWriter.append(line.toString() + "\n");
		}

		fileWriter.flush();
		fileWriter.close();
	}
	private static void writeGlobalFile(List<List<List<String>>> files) throws IOException {	
		FileWriter fileWriter = new FileWriter(WRITE_PATH + "/" + "all_data.csv");
		
		for (List<List<String>> file : files) {
			for (List<String> row : file) {
				StringBuffer line = new StringBuffer();
				for(String column: row) {
					line.append(column + CSV_SPLITER);
				}
				fileWriter.append(line.toString() + "\n");
			}
		}

		fileWriter.flush();
		fileWriter.close();
	}	
}
