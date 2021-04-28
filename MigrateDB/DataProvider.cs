using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net;
using System.Text;
using System.Web;
using MySql.Data.MySqlClient;

namespace MigrateDB
{
    public class DataProvider
    {

        public const string _SERVER = "14.241.225.1";
        public const string _PORT = "1433";
        public const string _USERNAME = "sa";
        public const string _PASSWORD = "L7G4khHjnHSzEEsm";
        private SqlConnection connection;
        public string DataBaseName { get; set; }

        public string TableName { get; set; }

        public List<string> Fields { get; set; }

        public DataProvider(string databaseName, string tableName, List<string> fields)
        {
            this.DataBaseName = databaseName;
            this.TableName = tableName;
            this.Fields = fields;
        }
        //
        public void ExcuteQuery(List<Query> querys)
        {
            try
            {
                string connectionString = "SERVER=" + "14.241.225.1" + ";" + "DATABASE=" + "eh" + ";" + "UID=" + "root" + ";" + "PASSWORD=" + "01Brickmate23!@" + ";Port=33070;";

                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();
                    foreach (var query in querys)
                    {
                        try
                        {
                            if (query.Line != null && !string.IsNullOrEmpty(query.QueryString))
                            {
                                using (MySqlCommand command = new MySqlCommand(query.QueryString, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine(query.Line);
                        }

                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public void CloseConnect()
        {
            connection.Close();
        }

        public List<string> GetAllData(string idx, int from, int to, int bm_idx)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = $"{_SERVER}";
            builder.UserID = _USERNAME;
            builder.Password = _PASSWORD;
            builder.InitialCatalog = this.DataBaseName;
            if (connection == null)
            {
                connection = new SqlConnection(builder.ConnectionString);
            }
            if (connection != null)
            {
                if (string.IsNullOrEmpty(this.DataBaseName) || string.IsNullOrEmpty(this.TableName) || this.Fields.Count <= 0)
                {
                    Console.WriteLine("Database name or table name not found!!!");
                    return null;
                }
                try
                {
                    var idlist = "";
                    List<string> result = new List<string>();

                    var fieldSelect = String.Join(",", Fields);
                    var sql = $"SELECT {fieldSelect} FROM {this.TableName} WHERE BM_IDX = {bm_idx} ORDER BY {idx} OFFSET {from} ROWS FETCH NEXT {to} ROWS ONLY";
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        if (connection.State == System.Data.ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var row = $"(";

                                #region Mapping data


                                //row += $"{reader.GetValue(0)}, ";
                                //row += $"'{reader.GetValue(1).ToString()}', ";
                                //row += $"'{reader.GetValue(2).ToString().Replace("#", "")}', ";
                                //row += $"'444444', ";
                                //row += $"'publish', ";
                                //row += ")";

                                row += "'[null]',";
                                row += $"{reader.GetValue(0)}, ";
                                row += $"'{WebUtility.HtmlDecode((reader.GetValue(1).ToString() + ' ').Replace("'", "")).Replace("'", "")}', ";
                                row += $"'{WebUtility.HtmlDecode((reader.GetValue(2).ToString() + ' ')).Replace("'", "").Replace("'", "")}', ";
                                row += $"{reader.GetValue(3)}, ";
                                row += $"{reader.GetValue(4)}, ";
                                row += $"'{reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss")}', ";
                                row += $"'{reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss")}', ";
                                row += $"'{reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss")}', ";

                                if (reader.GetValue(6).ToString() == "")
                                {
                                   row += "NULL)";
                                   // row += "0)";
                                }
                                else
                                {
                                    //row += $"{reader.GetValue(6)})";
                                    row += "'{" + "\"1\":" + "\"" + 1 + "\", \"2\": \"" + reader.GetValue(6) + "\"}')";
                                    //row += "0)";

                                }

                                idlist += reader.GetValue(0) + ",";



                                //row += $"'{WebUtility.HtmlDecode((reader.GetValue(0).ToString() + ' ').Replace("'", "")).Replace("'", "")}', ";
                                //row += $"'{WebUtility.HtmlDecode((reader.GetValue(1).ToString() + ' ')).Replace("'", "").Replace("'", "")}', ";
                                //row += $"{reader.GetValue(2)}, ";
                                //row += $"{reader.GetValue(3)}, ";
                                //row += $"'{reader.GetDateTime(4).ToString("yyyy-MM-dd HH:mm:ss")}', ";
                                //row += $"'{reader.GetDateTime(4).ToString("yyyy-MM-dd HH:mm:ss")}', ";
                                //row += "4)";

                                #endregion

                                if (!string.IsNullOrEmpty(row))
                                {
                                    result.Add(row);
                                }
                            }
                            if (!string.IsNullOrEmpty(idlist))
                            {
                                result.Add(idlist);
                            }
                            return result;
                        }
                    }

                }
                catch (SqlException e)
                {
                    Console.WriteLine(e.ToString());
                    return null;
                }
            }
            else
            {
                Console.WriteLine("Disconnected!!!");
                return null;
            }
        }

        public List<string> WhereIn(string idx, int from, int to, string bm_idx, string name, bool isCom)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = $"{_SERVER}";
            builder.UserID = _USERNAME;
            builder.Password = _PASSWORD;
            builder.InitialCatalog = this.DataBaseName;
            if (connection == null)
            {
                connection = new SqlConnection(builder.ConnectionString);
            }
            if (connection != null)
            {
                if (string.IsNullOrEmpty(this.DataBaseName) || string.IsNullOrEmpty(this.TableName) || this.Fields.Count <= 0)
                {
                    Console.WriteLine("Database name or table name not found!!!");
                    return null;
                }
                try
                {
                    var idlist = "";
                    List<string> result = new List<string>();

                    var fieldSelect = String.Join(",", Fields);
                    var sql = "";
                    var order = idx == "XXX" ? "BP_IDX,BR_NUM,BR_STEP" : idx;
                    if (!isCom)
                    {
                        sql = $"SELECT {fieldSelect} FROM {this.TableName} WHERE {name} IN {bm_idx} ORDER BY {order}  OFFSET {from} ROWS FETCH NEXT {to} ROWS ONLY ";
                    }
                    else
                    {
                        sql = $"SELECT {fieldSelect} FROM {this.TableName} WHERE {name} IN {bm_idx}";
                    }
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        if (connection.State == System.Data.ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            int parentId = 0;
                            while (reader.Read())
                            {
                                var row = $"(";

                                #region Mapping data
                     

                                if (!isCom)
                                {
                                    row += $"{reader.GetValue(5)},";
                                    row += $"{reader.GetValue(0)},";
                                    row += $"'{WebUtility.HtmlDecode((reader.GetValue(1).ToString() + ' ')).Replace("'", "").Replace("'", "")}', ";
                                    row += $"{reader.GetValue(2)},";
                                    row += $"'{reader.GetValue(3)}',";
                                    row += $"'{reader.GetDateTime(4).ToString("yyyy-MM-dd HH:mm:ss")}', ";
                                    row += $"'{reader.GetDateTime(4).ToString("yyyy-MM-dd HH:mm:ss")}',";

                                    // how can i get ID parent
                                    if (reader.GetInt32(6) == 0)
                                    {
                                        parentId = reader.GetInt32(5);
                                        row += $"NULL)";
                                    }
                                    else {
                                        row += $"{parentId})";
                                    }

                                    idlist += reader.GetValue(5) + ",";

                                }
                                else
                                {
                                    row += $"{reader.GetValue(0)},";
                                    row += $"{reader.GetValue(1)},";
                                    row += $"'{reader.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss")}', ";
                                    row += $"'{reader.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss")}',";
                                    if (this.TableName == "EWHA_POST_GUBAK_LOG" || this.TableName == "REPLE_BAD_LOG")
                                    {
                                        row += "1)";
                                    }
                                    else
                                    {
                                        row += "0)";
                                    }
                                }

                                #endregion

                                if (!string.IsNullOrEmpty(row))
                                {
                                    result.Add(row);
                                }
                            }
                            if (!isCom && !string.IsNullOrEmpty(idlist)) {
                                result.Add(idlist);
                            }
                            return result;
                        }
                    }

                }
                catch (SqlException e)
                {
                    Console.WriteLine(e.ToString());
                    return null;
                }
            }
            else
            {
                Console.WriteLine("Disconnected!!!");
                return null;
            }
        }
    }
}
