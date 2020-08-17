/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.potix2.spark.google.spreadsheets

import java.io.File

import org.scalatest.{BeforeAndAfter, FlatSpec}

class SparkSpreadsheetServiceReadSuite extends FlatSpec with BeforeAndAfter {
  private val TEST_SPREADSHEET_NAME = "TestSpreadsheet"
  private val TEST_SPREADSHEET_ID = ""
  private val CREDENTIALS_JSON = ""

  private val context: SparkSpreadsheetService.SparkSpreadsheetContext =
    SparkSpreadsheetService.SparkSpreadsheetContext(CREDENTIALS_JSON)
  private val spreadsheet: SparkSpreadsheetService.SparkSpreadsheet =
    context.findSpreadsheet(TEST_SPREADSHEET_ID)

  behavior of "A Spreadsheet"


  it should "have a name" in {
    assert(spreadsheet.name == TEST_SPREADSHEET_NAME)
  }

  behavior of "A worksheet"
  it should "be None when a worksheet is missing" in {
    assert(spreadsheet.findWorksheet("lolo").isEmpty)
  }

  it should "be retrieved when the worksheet exists" in {
    val worksheet = spreadsheet.findWorksheet("case2")
    assert(worksheet.isDefined)
    assert(worksheet.get.name == "case2")
    assert(worksheet.get.headers == List("id", "firstname", "lastname", "email", "country", "ipaddress"))

    val firstRow = worksheet.get.rows(0)
    assert(firstRow == Map(
      "id" -> "1",
      "firstname" -> "Annie",
      "lastname" -> "Willis",
      "email" -> "awillis0@princeton.edu",
      "country" -> "Burundi",
      "ipaddress" -> "241.162.49.104"))
  }
}
