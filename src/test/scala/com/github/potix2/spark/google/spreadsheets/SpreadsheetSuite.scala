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

import com.github.potix2.spark.google.spreadsheets.SparkSpreadsheetService.SparkSpreadsheetContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.util.Random

class SpreadsheetSuite extends FlatSpec with BeforeAndAfter {
  private val TEST_SPREADSHEET_ID = "1oToj6eQjuiG4wukOI7uTUQV3eroVlLIpb9LWzpJdx1w"
  private val CREDENTIALS_JSON =
    """
      |{
      |  "type": "service_account",
      |  "project_id": "testproject2-174820",
      |  "private_key_id": "59b7b380cac4dc1370d642a4cb487248f0d69262",
      |  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDHwnbrIlXOxXAw\n2gCaGD6aZvrUvbDXMohEYe7o7qWwSVTJLQo8dD4hs0Hng0WpdDxuGqKlIcEdYNBl\nqRKkQPQmmbjP6OR6r/mlgXMjcTpiLEn0Jjm+rJPCzWccD4Fy36YHSUaAEvE9yO/E\n817RHLDnaVmz/IqghEHo7rFMFpZ7+NcQU21etsSH/xhtIi+eEdUzrWu55asarprP\nhglkaL7OHYo4fW9dTZwQtBtRRZnVWhXtKy1VcPHOMQPM+JLpoUzV718au8FdvdhL\nX/8KpO8LWa+o4phuF7kqn3lzuErTAx+pEQYUqIFJ3CEr55rAW3HCgAxfvZkmD5pQ\n9JrNTpaFAgMBAAECggEAJK26yyLjUZOkLXwh5ylzeUNWZDCuY10mczPuO7vyFWPp\nmwTXn9ESRXrWK43JgTtUCz19xsdjX6MSsM/yGdHJYrsQGbDgHvzn4HFb9FKKj1Ml\ngxxvtuiWOwsfFyJruO2C3UyhksunmxolmGq9arUTcHJCI8/HnoCaX4xQZrGxEPME\n8rQldvLJWAxZLbACiL0OtDp+dhS0UdyXuAZieAijGWkNs+lB7Ju9NlRO7d3vfRx5\n/JJdxaomUImpsdoKdIyEfgo3w8aWcfcFOtJU4UN0IxWnZeUq8SfW3U3upyOmfEEG\nyrdvb6ml7CKA43aMR2exAMjZP0CEX9trJ1EqQbaQ4wKBgQD8V1DmuqyhD1m6lGw4\nSB0e+Fx9/sCL5snigZo/p9gxvp7bX1EQi/hiNodx9IJXZWuUq1ANgjYg/ojKV7or\nVn7BuSS6YGQsOQa/r7JqJCROGUOfdB2Irch0Gcz3q8V2Li8YX5NSLW14GAjJUNzv\ncWm1QjfSlq6YzTGccy26CG8YswKBgQDKp/epeY+SuhSdJXlMfXWs+2pmcBONPaOG\nM6VBILAuuxkOEj3UQ2vt2YD/IppHQTm3g/JTrgCDzl7bNs90VlLuHAriYUK4AqKV\nepiEIm9IKy3j90aqdYaXnb7GuncOYiUSEVraT68SpRbgd4t3lO+qAQDXIBWtvjs0\nBlbpYzf/5wKBgQDb3vkPEfj5HXcBq/Hf2HYHVkDBSAhd3mpqgqL0dDtcnMuuOg9Y\na52xdfHuyS4JGMX0dJD8NEkV1rM6G9aLjIJGKhxmiTa/kbDftSewdG1t33WcqoBR\nZ5sSDqkZ5QHZR8ShaCXlpM+NpOODBoJ74EZLObeKNuFLIv/nYjSltFOZIQKBgCtF\n1oJfrKKeDUzI9a+5kmkPflbFU8dzA/niCUVw3237EYyJpJ4wj1lIel0AXIUejl57\nVEE+BGogpOyWNZIX5LlnT4OrVP3JwkG83dhJKg30+mWVJYe8dLLnQhdZNZbiqhvD\njOzU2wNWMR5ZKHSuXsakVDObbEWfOzEOsXxAbXK9AoGBAKj7nfFJBTcb1d9ckuiv\nmnirQ6/I4CsL7zz1JG6veck7MqD5WPSMGByGyk9mKpDDDr4Im9whkHTYdsRp2/XI\nJFg6BVpQFUfHkTtFu7GbE5ZqBKlKlvBHd/6vJMYahVAaD8FCszmdq+UWvQVJRED5\ng+pcRfRdl9fyw8kai+VUnpDR\n-----END PRIVATE KEY-----\n",
      |  "client_email": "test2-224@testproject2-174820.iam.gserviceaccount.com",
      |  "client_id": "109293901349385915420",
      |  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      |  "token_uri": "https://accounts.google.com/o/oauth2/token",
      |  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      |  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test2-224%40testproject2-174820.iam.gserviceaccount.com"
      |}
    """.stripMargin

  private var sqlContext: SQLContext = _
  before {
    sqlContext = new SQLContext(new SparkContext("local[2]", "SpreadsheetSuite"))
  }

  after {
    sqlContext.sparkContext.stop()
  }

  private[spreadsheets] def deleteWorksheet(spreadSheetName: String, worksheetName: String)
                                           (implicit spreadSheetContext: SparkSpreadsheetContext): Unit = {
    SparkSpreadsheetService
      .findSpreadsheet(spreadSheetName)
      .foreach(_.deleteWorksheet(worksheetName))
  }

  def withNewEmptyWorksheet(testCode:(String) => Any): Unit = {
    implicit val spreadSheetContext = SparkSpreadsheetService(CREDENTIALS_JSON)
    val spreadsheet = SparkSpreadsheetService.findSpreadsheet(TEST_SPREADSHEET_ID)
    spreadsheet.foreach { s =>
      val workSheetName = Random.alphanumeric.take(16).mkString
      s.addWorksheet(workSheetName, 1000, 1000)
      try {
        testCode(workSheetName)
      }
      finally {
        s.deleteWorksheet(workSheetName)
      }
    }
  }

  def withEmptyWorksheet(testCode:(String) => Any): Unit = {
    implicit val spreadSheetContext = SparkSpreadsheetService(CREDENTIALS_JSON)
    val workSheetName = Random.alphanumeric.take(16).mkString
    try {
      testCode(workSheetName)
    }
    finally {
      deleteWorksheet(TEST_SPREADSHEET_ID, workSheetName)
    }
  }

  behavior of "A sheet"

  it should "behave as a DataFrame" in {
    val results = sqlContext.read
      .option("client_json", CREDENTIALS_JSON)
      .spreadsheet(s"$TEST_SPREADSHEET_ID/case1")
      .select("col1")
      .collect()

    assert(results.size === 15)
  }

  it should "have a `long` value" in {
    val schema = StructType(Seq(
      StructField("col1", DataTypes.LongType),
      StructField("col2", DataTypes.StringType),
      StructField("col3", DataTypes.StringType)
    ))

    val results = sqlContext.read
      .option("client_json", CREDENTIALS_JSON)
      .schema(schema)
      .spreadsheet(s"$TEST_SPREADSHEET_ID/case1")
      .select("col1", "col2", "col3")
      .collect()

    assert(results.head.getLong(0) === 1L)
    assert(results.head.getString(1) === "2")
    assert(results.head.getString(2) === "3")
  }

  trait PersonData {
    val personsSchema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("firstname", StringType, true),
      StructField("lastname", StringType, true)))

    val secondPersonsSchema = StructType(List(
      StructField("leadId", IntegerType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("leadScore", LongType, true)))
  }

  trait PersonDataFrame extends PersonData {
    val personsRows = Seq(Row(1, "Kathleen", "Cole"), Row(2, "Julia", "Richards"), Row(3, "Terry", ""))
    val nextPersonsRows = Seq(Row(1, "John", "Snow"), Row(2, "Knows", "Nothing"))
    val secondPersonsRows = Seq(Row(1, "John", "Thomas", ""), Row(2, "John", "Thomas", 32), Row("", "Anne", "Jacobs", 32), Row(4, "Anne", "Jacobs", ""))
    val personsRDD = sqlContext.sparkContext.parallelize(personsRows)
    val nextPersonsRDD = sqlContext.sparkContext.parallelize(nextPersonsRows)
    val personsDF = sqlContext.createDataFrame(personsRDD, personsSchema)
    val nextPersonsDF = sqlContext.createDataFrame(nextPersonsRDD, personsSchema)
    val secondPersonsRDD = sqlContext.sparkContext.parallelize(secondPersonsRows)
    val secondPersonsDF = sqlContext.createDataFrame(secondPersonsRDD, secondPersonsSchema)
  }

  trait SparsePersonDataFrame extends PersonData {
    val RowCount = 10

    def firstNameValue(id: Int): String = {
      if (id % 3 != 0) s"first-${id}" else null
    }

    def lastNameValue(id: Int): String = {
      if (id % 4 != 0) s"last-${id}" else null
    }

    val personsRows = (1 to RowCount) map { id: Int =>
      Row(id, firstNameValue(id), lastNameValue(id))
    }
    val personsRDD = sqlContext.sparkContext.parallelize(personsRows)
    val personsDF = sqlContext.createDataFrame(personsRDD, personsSchema)
  }

  behavior of "A DataFrame"

  it should "be saved as a sheet" in new PersonDataFrame {
    import com.github.potix2.spark.google.spreadsheets._
    withEmptyWorksheet { workSheetName =>
      personsDF.write
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")

      val result = sqlContext.read
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")
        .collect()

      assert(result.size == 3)
      assert(result(0).getString(0) == "1")
      assert(result(0).getString(1) == "Kathleen")
      assert(result(0).getString(2) == "Cole")
    }
  }

  it should "infer it's schema from headers" in {
    val results = sqlContext.read
      .option("client_json", CREDENTIALS_JSON)
      .spreadsheet(s"$TEST_SPREADSHEET_ID/case3")

    assert(results.columns.size === 2)
    assert(results.columns.contains("a"))
    assert(results.columns.contains("b"))
  }

  "A sparse DataFrame" should "be saved as a sheet, preserving empty cells" in new SparsePersonDataFrame {
    import com.github.potix2.spark.google.spreadsheets._
    withEmptyWorksheet { workSheetName =>
      personsDF.write
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")

      val result = sqlContext.read
        .schema(personsSchema)
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")
        .collect()

      assert(result.size == RowCount)

      (1 to RowCount) foreach { id: Int =>
        val row = id - 1
        val first = firstNameValue(id)
        val last = lastNameValue(id)
        // TODO: further investigate/fix null handling
        // assert(result(row) == Row(id, if (first == null) "" else first, if (last == null) "" else last))
      }
    }
  }

  "A table" should "be created from DDL with schema" in {
    withNewEmptyWorksheet { worksheetName =>
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE people
           |(id int, firstname string, lastname string)
           |USING com.github.potix2.spark.google.spreadsheets
           |OPTIONS (path "$TEST_SPREADSHEET_ID/$worksheetName", client_json "$CREDENTIALS_JSON")
       """.stripMargin.replaceAll("\n", " "))

      assert(sqlContext.sql("SELECT * FROM people").collect().size == 0)
    }
  }

  it should "be created from DDL with inferred schema" in {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE SpreadsheetSuite
         |USING com.github.potix2.spark.google.spreadsheets
         |OPTIONS (path "$TEST_SPREADSHEET_ID/case2", client_json "$CREDENTIALS_JSON")
       """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT id, firstname, lastname FROM SpreadsheetSuite").collect().size == 10)
  }

  it should "be overwritten as a sheet" in new PersonDataFrame {
    withEmptyWorksheet { workSheetName =>
      personsDF
        .write
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")

      nextPersonsDF
        .write
        .option("client_json", CREDENTIALS_JSON)
        .mode(SaveMode.Overwrite)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")

      val result = sqlContext.read
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")
        .collect()

      assert(result.size == 2)
      assert(result(0).getString(0) == "1")
      assert(result(0).getString(1) == "John")
      assert(result(0).getString(2) == "Snow")
    }
  }

  it should "be appended as a sheet" in new PersonDataFrame {
    /*withEmptyWorksheet { workSheetName =>*/
      /*personsDF
        .write
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")*/

    secondPersonsDF
        .write
        .option("client_json", CREDENTIALS_JSON)
        .mode(SaveMode.Append)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/Sheet6")

      val result = sqlContext.read
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/Sheet6")
        .collect()


      /*assert(result.size == 5)
      assert(result(4).getString(0) == "2")
      assert(result(4).getString(1) == "Knows")
      assert(result(4).getString(2) == "Nothing")*/
    //}
  }

  it should "be inserted from sql" in {
    withNewEmptyWorksheet { worksheetName =>
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE accesslog
           |(id string, firstname string, lastname string, email string, country string, ipaddress string)
           |USING com.github.potix2.spark.google.spreadsheets
           |OPTIONS (path "$TEST_SPREADSHEET_ID/$worksheetName", client_json "$CREDENTIALS_JSON")
       """.stripMargin.replaceAll("\n", " "))

      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE SpreadsheetSuite
           |USING com.github.potix2.spark.google.spreadsheets
           |OPTIONS (path "$TEST_SPREADSHEET_ID/case2", client_json "$CREDENTIALS_JSON")
       """.stripMargin.replaceAll("\n", " "))

      sqlContext.sql("INSERT OVERWRITE TABLE accesslog SELECT * FROM SpreadsheetSuite")
      assert(sqlContext.sql("SELECT id, firstname, lastname FROM accesslog").collect().size == 10)
    }
  }

  trait UnderscoreDataFrame {
    val aSchema = StructType(List(
      StructField("foo_bar", IntegerType, true)))
    val aRows = Seq(Row(1), Row(2), Row(3))
    val aRDD = sqlContext.sparkContext.parallelize(aRows)
    val aDF = sqlContext.createDataFrame(aRDD, aSchema)
  }

  "The underscore" should "be used in a column name" in new UnderscoreDataFrame {
    import com.github.potix2.spark.google.spreadsheets._
    withEmptyWorksheet { workSheetName =>
      aDF.write
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")

      val result = sqlContext.read
        .option("client_json", CREDENTIALS_JSON)
        .spreadsheet(s"$TEST_SPREADSHEET_ID/$workSheetName")
        .collect()

      assert(result.size == 3)
      assert(result(0).getString(0) == "1")
    }
  }
}
