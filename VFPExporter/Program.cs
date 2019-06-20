using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace VFPExporter
{
    class Program
    {
        private const string _sqlConnectionString = @"Data Source=localhost; Initial Catalog=TestDB; Integrated Security=SSPI;";

        static int rowCount = 0;

        static DateTime start = DateTime.UtcNow;

        static void Main(string[] args)
        {
            start = DateTime.UtcNow;
            var loadList = new List<string>();

            foreach (var fileName in Directory.GetFiles(@"Z:\TestData\", "*.dbc", SearchOption.AllDirectories))
            {
                if(loadList.Contains(Path.GetFileName(fileName)))
                    LoadFile(fileName);
            }
        }

        public static DataTable LoadFile(string fileName)
        {
            var tables = new List<string>();

            Console.WriteLine("Loading File: {0}", fileName);

            var connectionString = @"Provider=VFPOLEDB.1;Data Source=" + fileName;

            string schemaName = Path.GetFileNameWithoutExtension(fileName);

            using (var fpConn = new OleDbConnection(connectionString))
            {
                fpConn.Open();

                DataTable schemaTable = fpConn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });

                foreach (DataRow row in schemaTable.Rows)
                {
                    var tableName = (string)row["TABLE_NAME"];


                    var fullTableName = "[" + schemaName + "].[" + tableName + "]";

                    try
                    {
                        OleDbCommand schemaCommand = new OleDbCommand("select * from " + tableName, fpConn);
                        DataTable dtSchema = null;
                        using (var reader = schemaCommand.ExecuteReader(CommandBehavior.SchemaOnly))
                        {
                            dtSchema = reader.GetSchemaTable();
                        }

                        using (var sqlConn = new SqlConnection(_sqlConnectionString))
                        {
                            sqlConn.Open();

                            int schemaId = GetSchemaId(schemaName, sqlConn);

                            if (TableExists(tableName, schemaId, sqlConn))
                                DropTable(fullTableName, sqlConn);

                            CreateTable(dtSchema, fullTableName, sqlConn);

                            sqlConn.Close();
                        }


                        Console.WriteLine("Reading and Writing: {0}", fullTableName);

                        var countCmd = new OleDbCommand("SELECT max(recno()) FROM " + tableName, fpConn);

                        var countObj = countCmd.ExecuteScalar();

                        int count = countObj is DBNull ? 0 : Convert.ToInt32(countObj);

                        rowCount += count;

                        Console.WriteLine("Rows: " + count);

                        var selectString = MakeSelectStatement(dtSchema, tableName);
                        if (count > 100000)
                        {
                            int half = count / 2;

                            var first = new OleDbCommand(selectString + " WHERE recNo() < " + half, fpConn);
                            var second = new OleDbCommand(selectString + " WHERE recNo() >= " + half, fpConn);

                            SqlBulkCopyFromOleCommand(_sqlConnectionString, fullTableName, first);
                            SqlBulkCopyFromOleCommand(_sqlConnectionString, fullTableName, second);
                        }
                        else
                        {
                            var first = new OleDbCommand(selectString, fpConn);

                            SqlBulkCopyFromOleCommand(_sqlConnectionString, fullTableName, first);
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error Migrating: " + fullTableName);
                    }

                    DateTime end = DateTime.UtcNow;
                    TimeSpan delta = end.Subtract(start);

                    double rowsPerSecond = ((double)rowCount / (double)delta.Milliseconds);
                    Console.WriteLine("Rows/MS: " + rowsPerSecond);

                }

                fpConn.Close();
            }

            return null;
        }

        public static int GetSchemaId(string schemaName, SqlConnection sqlConn)
        {
            SqlCommand getSchemaId = new SqlCommand("SELECT TOP 1 schema_id FROM sys.schemas where name = '" + schemaName + "'", sqlConn);
            var result = getSchemaId.ExecuteScalar();

            if (result == null)
            {
                SqlCommand createSchema = new SqlCommand("CREATE SCHEMA " + schemaName + ";", sqlConn);
                createSchema.ExecuteNonQuery();
                result = getSchemaId.ExecuteScalar();
            }

            return Convert.ToInt32(result);
        }

        public static int GetTableCount(string tableName, SqlConnection sqlConn)
        {
            SqlCommand cmd = new SqlCommand("SELECT count(*) FROM " + tableName, sqlConn);
            var result = cmd.ExecuteScalar();

            return (int)result;
        }

        public static bool TableExists(string tableName, int schemaId, SqlConnection sqlConn)
        {
            SqlCommand cmd = new SqlCommand("SELECT TOP 1 'EXISTS' FROM sys.objects where type = 'U' and schema_id = " + schemaId + " and name = '" + tableName + "'", sqlConn);
            var result = cmd.ExecuteScalar();

            if (result == null)
                return false;

            return true;
        }

        public static void CreateTable(DataTable dt, string fullTableName, SqlConnection sqlConn)
        {
            //string createTableString = "CREATE TABLE [" + fullTableName.Replace(".", "].[") + "](";
            string createTableString = "CREATE TABLE " + fullTableName + "(";
            bool hasPk = false;

            foreach (DataRow row in dt.Rows)
            {
                string columnName = (string)row["ColumnName"];
                int precision = 18;//((short)row["NumericPrecision"]) + 1;
                int scale = (short)row["NumericScale"];
                int width = (int)row["ColumnSize"];
                string type = row["DataType"].ToString();

                string strWidth = width.ToString();
                if (width > 8000)
                    strWidth = "max";

                createTableString += "\n [" + columnName + "] ";

                switch (type)
                {
                    case "System.Int32":
                        createTableString += " int ";
                        break;
                    case "System.Int64":
                        createTableString += " bigint ";
                        break;
                    case "System.Int16":
                        createTableString += " smallint";
                        break;
                    case "System.Byte":
                        createTableString += " tinyint";
                        break;
                    case "System.Decimal":
                        createTableString += " decimal(" + precision + ", " + scale + ")";
                        break;
                    case "System.DateTime":
                        createTableString += " datetime2 ";
                        break;
                    case "System.Boolean":
                        createTableString += " bit ";
                        break;
                    case "System.String":
                        createTableString += " nvarchar(" + strWidth + ") ";
                        break;
                    case "System.Byte[]":
                        createTableString += " varbinary(max) ";
                        break;
                    default:
                        Console.WriteLine("Warning: {0} is {1}", columnName, type);

                        createTableString += " nvarchar(max) ";
                        break;
                }


                createTableString += ",";
            }

            if(hasPk)
            {
                createTableString += ("CONSTRAINT[PK_" + fullTableName + "] PRIMARY KEY CLUSTERED ([pk] ASC) WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]) ON[PRIMARY]");
            }
            else
            {
                createTableString = createTableString.Substring(0, createTableString.Length - 1) + "\n)";
            }

            SqlCommand createtable = new SqlCommand(createTableString, sqlConn);
            createtable.ExecuteNonQuery();
        }

        public static string MakeSelectStatement(DataTable schemaTable, string tableName)
        {
            string selectString = "SELECT ";
            foreach (DataRow row in schemaTable.Rows)
            {
                string columnName = (string)row["ColumnName"];
                int precision = 18;//((short)row["NumericPrecision"]) + 1;
                int scale = (short)row["NumericScale"];
                string type = row["DataType"].ToString();

                if (type == "System.Decimal")
                {
                    selectString += String.Format(" CAST({0} AS NUMERIC({1},{2})),\n", columnName, precision, scale);
                }
                else
                {
                    selectString += (" " + columnName + ",\n");
                }
            }

            return selectString.Substring(0, selectString.Length - 2) + " \nFROM " + tableName;
        }

        public static void DropTable(string fullTableName, SqlConnection sqlConn)
        {
            SqlCommand droptable = new SqlCommand("DROP TABLE " + fullTableName, sqlConn);
            droptable.ExecuteNonQuery();
        }

        private static void OnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine("Copied {0} so far...", e.RowsCopied);
        }

        private static void SqlBulkCopyFromOleCommand(string destConnectionString, string tableName, OleDbCommand srcOleDbCommand)
        {
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(destConnectionString, SqlBulkCopyOptions.TableLock))
            {
                bulkcopy.BatchSize = 40000;
                bulkcopy.EnableStreaming = true;
                bulkcopy.DestinationTableName = tableName;

                using (var reader = srcOleDbCommand.ExecuteReader())
                {
                    bulkcopy.WriteToServer(reader);
                }
            }
        }
    }
}
