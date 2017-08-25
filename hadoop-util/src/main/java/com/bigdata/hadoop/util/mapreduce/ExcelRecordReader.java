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
package com.bigdata.hadoop.util.mapreduce;

import com.bigdata.hadoop.util.parser.ExcelParser;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads excel spread sheet , where keys are offset in file and value is the row
 * containing all column as a string.
 */
public class ExcelRecordReader extends TextRecordReader{

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		super.initialize(genericSplit, context);
	}

	@Override
	protected String readSplitFile(TaskAttemptContext context,InputStream is) {
		return new ExcelParser(context).parseExcelData(is);
	}
}
