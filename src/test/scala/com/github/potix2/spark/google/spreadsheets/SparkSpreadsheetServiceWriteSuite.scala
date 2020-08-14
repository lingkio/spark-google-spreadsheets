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

import com.github.potix2.spark.google.spreadsheets.SparkSpreadsheetService.SparkSpreadsheet
import com.google.api.services.sheets.v4.model.{ExtendedValue, CellData, RowData}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.JavaConverters._

class SparkSpreadsheetServiceWriteSuite extends FlatSpec with BeforeAndAfter {
  private val TEST_SPREADSHEET_NAME = "TestSpreadsheet"
  private val TEST_SPREADSHEET_ID = "1oToj6eQjuiG4wukOI7uTUQV3eroVlLIpb9LWzpJdx1w"
  private val CREDENTIALS_JSON = "\n    { \n          \"type\": \"service_account\",\n          \"project_id\": \"testproject2-174820\",\n          \"private_key_id\": \"59b7b380cac4dc1370d642a4cb487248f0d69262\",\n          \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDHwnbrIlXOxXAw\\n2gCaGD6aZvrUvbDXMohEYe7o7qWwSVTJLQo8dD4hs0Hng0WpdDxuGqKlIcEdYNBl\\nqRKkQPQmmbjP6OR6r/mlgXMjcTpiLEn0Jjm+rJPCzWccD4Fy36YHSUaAEvE9yO/E\\n817RHLDnaVmz/IqghEHo7rFMFpZ7+NcQU21etsSH/xhtIi+eEdUzrWu55asarprP\\nhglkaL7OHYo4fW9dTZwQtBtRRZnVWhXtKy1VcPHOMQPM+JLpoUzV718au8FdvdhL\\nX/8KpO8LWa+o4phuF7kqn3lzuErTAx+pEQYUqIFJ3CEr55rAW3HCgAxfvZkmD5pQ\\n9JrNTpaFAgMBAAECggEAJK26yyLjUZOkLXwh5ylzeUNWZDCuY10mczPuO7vyFWPp\\nmwTXn9ESRXrWK43JgTtUCz19xsdjX6MSsM/yGdHJYrsQGbDgHvzn4HFb9FKKj1Ml\\ngxxvtuiWOwsfFyJruO2C3UyhksunmxolmGq9arUTcHJCI8/HnoCaX4xQZrGxEPME\\n8rQldvLJWAxZLbACiL0OtDp+dhS0UdyXuAZieAijGWkNs+lB7Ju9NlRO7d3vfRx5\\n/JJdxaomUImpsdoKdIyEfgo3w8aWcfcFOtJU4UN0IxWnZeUq8SfW3U3upyOmfEEG\\nyrdvb6ml7CKA43aMR2exAMjZP0CEX9trJ1EqQbaQ4wKBgQD8V1DmuqyhD1m6lGw4\\nSB0e+Fx9/sCL5snigZo/p9gxvp7bX1EQi/hiNodx9IJXZWuUq1ANgjYg/ojKV7or\\nVn7BuSS6YGQsOQa/r7JqJCROGUOfdB2Irch0Gcz3q8V2Li8YX5NSLW14GAjJUNzv\\ncWm1QjfSlq6YzTGccy26CG8YswKBgQDKp/epeY+SuhSdJXlMfXWs+2pmcBONPaOG\\nM6VBILAuuxkOEj3UQ2vt2YD/IppHQTm3g/JTrgCDzl7bNs90VlLuHAriYUK4AqKV\\nepiEIm9IKy3j90aqdYaXnb7GuncOYiUSEVraT68SpRbgd4t3lO+qAQDXIBWtvjs0\\nBlbpYzf/5wKBgQDb3vkPEfj5HXcBq/Hf2HYHVkDBSAhd3mpqgqL0dDtcnMuuOg9Y\\na52xdfHuyS4JGMX0dJD8NEkV1rM6G9aLjIJGKhxmiTa/kbDftSewdG1t33WcqoBR\\nZ5sSDqkZ5QHZR8ShaCXlpM+NpOODBoJ74EZLObeKNuFLIv/nYjSltFOZIQKBgCtF\\n1oJfrKKeDUzI9a+5kmkPflbFU8dzA/niCUVw3237EYyJpJ4wj1lIel0AXIUejl57\\nVEE+BGogpOyWNZIX5LlnT4OrVP3JwkG83dhJKg30+mWVJYe8dLLnQhdZNZbiqhvD\\njOzU2wNWMR5ZKHSuXsakVDObbEWfOzEOsXxAbXK9AoGBAKj7nfFJBTcb1d9ckuiv\\nmnirQ6/I4CsL7zz1JG6veck7MqD5WPSMGByGyk9mKpDDDr4Im9whkHTYdsRp2/XI\\nJFg6BVpQFUfHkTtFu7GbE5ZqBKlKlvBHd/6vJMYahVAaD8FCszmdq+UWvQVJRED5\\ng+pcRfRdl9fyw8kai+VUnpDR\\n-----END PRIVATE KEY-----\\n\",\n          \"client_email\": \"test2-224@testproject2-174820.iam.gserviceaccount.com\",\n          \"client_id\": \"109293901349385915420\",\n          \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n          \"token_uri\": \"https://accounts.google.com/o/oauth2/token\",\n          \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n          \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/test2-224%40testproject2-174820.iam.gserviceaccount.com\"\n        }"

  private val context: SparkSpreadsheetService.SparkSpreadsheetContext =
    SparkSpreadsheetService.SparkSpreadsheetContext(CREDENTIALS_JSON)
  var spreadsheet: SparkSpreadsheet = null
  var worksheetName: String = ""

  def definedSchema: StructType = {
    new StructType()
      .add(new StructField("col_1", DataTypes.StringType))
      .add(new StructField("col_2", DataTypes.LongType))
      .add(new StructField("col_3", DataTypes.StringType))
  }

  case class Elem(col_1: String, col_2: Long, col_3: String)

  def extractor(e: Elem): RowData =
    new RowData().setValues(
      List(
        new CellData().setUserEnteredValue(
          new ExtendedValue().setStringValue(e.col_1)
        ),
        new CellData().setUserEnteredValue(
          new ExtendedValue().setNumberValue(e.col_2.toDouble)
        ),
        new CellData().setUserEnteredValue(
          new ExtendedValue().setStringValue(e.col_3)
        )
      ).asJava
    )

  before {
    spreadsheet = context.findSpreadsheet(TEST_SPREADSHEET_ID)
    worksheetName = scala.util.Random.alphanumeric.take(16).mkString
    val data = List(
      Elem("a", 1L, "x"),
      Elem("b", 2L, "y"),
      Elem("c", 3L, "z")
    )

    spreadsheet.addWorksheet(worksheetName, definedSchema, data, extractor)
  }

  after {
    spreadsheet.deleteWorksheet(worksheetName)
  }

  behavior of "A Spreadsheet"
  it should "find the new worksheet" in {
    val newWorksheet = spreadsheet.findWorksheet(worksheetName)
    assert(newWorksheet.isDefined)
    assert(newWorksheet.get.name == worksheetName)
    assert(newWorksheet.get.headers == Seq("col_1", "col_2", "col_3"))

    val rows = newWorksheet.get.rows
    assert(rows.head == Map("col_1" -> "a", "col_2" -> "1", "col_3" -> "x"))
  }

  behavior of "SparkWorksheet#updateCells"
  it should "update values in a worksheet" in {
    val newWorksheet = spreadsheet.findWorksheet(worksheetName)
    assert(newWorksheet.isDefined)

    val newData = List(
      Elem("f", 5L, "yy"),
      Elem("e", 4L, "xx"),
      Elem("c", 3L, "z"),
      Elem("b", 2L, "y"),
      Elem("a", 1L, "x")
    )

    newWorksheet.get.updateCells(definedSchema, newData, extractor)

    val rows = newWorksheet.get.rows
    assert(rows.head == Map("col_1" -> "f", "col_2" -> "5", "col_3" -> "yy"))
    assert(rows.last == Map("col_1" -> "a", "col_2" -> "1", "col_3" -> "x"))
  }
}
