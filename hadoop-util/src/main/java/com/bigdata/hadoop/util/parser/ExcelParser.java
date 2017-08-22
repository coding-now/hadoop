/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bigdata.hadoop.util.parser;

import com.bigdata.hadoop.util.mapreduce.ExcelConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ExcelParser {

	private static final Logger logger = LoggerFactory.getLogger(ExcelParser.class);
	private StringBuilder currentString = null;
	private long bytesRead = 0;
	private TaskAttemptContext context;

	public ExcelParser(TaskAttemptContext context) {
		this.context = context;
	}

	public String parseExcelData(InputStream is) {
		try {
			HSSFWorkbook workbook = new HSSFWorkbook(is);
			// Taking first sheet from the workbook
			HSSFSheet sheet = workbook.getSheetAt(0);
			// Iterate through each rows from first sheet
			Iterator<Row> rowIterator = sheet.iterator();
			currentString = new StringBuilder();
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				// For each row, iterate through each columns
				Iterator<Cell> cellIterator = row.cellIterator();
				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					try {
						currentString.append(getCellContent(cell)).append("\t");
					}catch (Exception ex){
						logger.error("error-line:{}",cell.getAddress(),ex);
						context.getCounter(ExcelConfig.LineCounter.EXCEL_LINE_SKIP).increment(1);
					}
				}
				currentString.append("\n");
			}
			is.close();
		} catch (IOException e) {
			logger.error("IO Exception : File not found " + e);
		}
		return currentString.toString();

	}

	public long getBytesRead() {
		return bytesRead;
	}

	private Object getCellContent(Cell cell){
		if(cell == null) return null;
		if(cell instanceof XSSFCell) {//xssf
			try {
				if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
					return (""+cell.getBooleanCellValue());
				} else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
					//return (cell.getNumericCellValue());
					if (DateUtil.isCellDateFormatted(cell)) {
						return (cell.getDateCellValue());
					} else {
						double doubleVal = cell.getNumericCellValue();
						long longVal = Math.round(doubleVal);
						if(Double.parseDouble(longVal + ".0") == doubleVal)
							return longVal;
						else
							return doubleVal;
						//return cell.getNumericCellValue();
					}
				} else if (cell.getCellType() == Cell.CELL_TYPE_BLANK) {
					return null;
				} else if (cell.getCellType() == Cell.CELL_TYPE_ERROR) {
					logger.debug("error-cell:{}", cell.getAddress().formatAsString());
					return ((XSSFCell) cell).getErrorCellString();
				}else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
					//return cell.getCellFormula();
					try {
						return (((XSSFCell) cell).getRawValue());
					} catch (IllegalStateException e) {
						logger.error("formula-cell-error:{},val={}",
								cell.getAddress().formatAsString(),((XSSFCell) cell).getRawValue());
						return String.valueOf(((XSSFCell) cell).getRawValue());
					}
				}
				return (cell.getStringCellValue());
			}catch (Throwable ex){
				logger.error("getCellValueError:{}",cell.getAddress().formatAsString(),ex);
				return null;
			}
		}else {
			HSSFCell hssfCell = (HSSFCell)cell;
			if (hssfCell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
				return String.valueOf(hssfCell.getBooleanCellValue());
			} else if (hssfCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return String.valueOf(hssfCell.getNumericCellValue());
			} else {
				return String.valueOf(hssfCell.getStringCellValue());
			}
		}
	}
}
