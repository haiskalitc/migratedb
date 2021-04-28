using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MigrateDB
{
    public class Worker
    {
        /// <summary>
        /// Runner
        /// </summary>
        public static void Runner()
        {
            EventComment();
        }

        #region BM_IDX = 1

        /// <summary>
        /// BM_IDX = 1
        /// Move to events_cmt
        /// Count 75
        /// </summary>
        public static void EventCMT()
        {
            DataProvider dataProvider = new DataProvider(
                "EWHAIAN",
                "EWHA_BOARD_POST",
                new List<string>() { "BP_TITLE", "BP_CONTENT", "BP_COUNT", "MEM_IDX", "BP_DATE" });
            var query = $"INSERT INTO `` (title, detail, views, member_id, created_at, updated_at) VALUES";
            var dsQuery = GetQuery(dataProvider, query, 0, 500, 1);
            dataProvider.ExcuteQuery(dsQuery);

            Console.WriteLine("Done!!");
        }

        #endregion

        #region BM_IDX = 4
        /// <summary>
        /// BM_IDX = 4
        /// Move to events and comments
        /// Count 161
        /// </summary>
        public static void EventComment()
        {
            for (int i = 7900; i < 8000; i += 100)
            {
                var idlist = "";
                var idlistComment = "";
                try
                {
                    DataProvider dataProvider = new DataProvider(
                   "EWHAIAN",
                   "EWHA_BOARD_POST",
                         new List<string>() { "BP_IDX", "BP_TITLE", "BP_CONTENT", "BP_COUNT", "MEM_IDX", "BP_DATE", "CT_IDX" });
                    var query = $"INSERT INTO `jobs_part_time` (link, id, title, detail, lookup, member_id, created_at, updated_at,published, categories) VALUES ";
                    var dsQuery = GetQuery(dataProvider, query, i, i + 100, 2);
                    idlist = "(" + dsQuery[dsQuery.Count - 1].QueryString + "0)";
                  dataProvider.ExcuteQuery(dsQuery);
                }
                catch
                {

                }
                finally
                {
                    // POST_COMMENT
                    DataProvider dataProvider = new DataProvider(
                        "EWHAIAN",
                        "EWHA_BOARD_REPLE",
                              new List<string>() { "BP_IDX", "BR_CONTENT", "MEM_IDX", "MEM_IP", "BR_DATE", "BR_IDX", "BR_STEP" });
                    var query = $"INSERT INTO `jobs_comments` (id, jobs_part_time_id, content, member_id, ip_address, created_at, updated_at, parents_id) VALUES ";
                    var dsQuery = GetQueryIn(dataProvider, query, 0, 3000, idlist, "BP_IDX", false);
                    idlistComment = "(" + dsQuery[dsQuery.Count - 1].QueryString + "0)";
                     dataProvider.ExcuteQuery(dsQuery);

                    // POST_SYMPATHY

                    if (!string.IsNullOrEmpty(idlist))
                    {
                        try
                        {
                            DataProvider dataProviderSympathy = new DataProvider(
                                "EWHAIAN_LOG",
                                "EWHA_POST_GOOD_LOG",
                                new List<string>() { "BP_IDX", "MEM_IDX", "L_DATE" });
                            var querySympathy = $"INSERT INTO `sympathy_jobs_part_time` (jobs_part_time_id, member_id, created_at, updated_at, is_dislike) VALUES ";
                            var dsQuerySympathy = GetQueryIn(dataProviderSympathy, querySympathy, 0, 3000, idlist, "BP_IDX", true);
                            dataProvider.ExcuteQuery(dsQuerySympathy);
                        }
                        catch
                        {

                        }
                        finally
                        {
                            DataProvider dataProviderSympathy = new DataProvider(
                            "EWHAIAN_LOG",
                            "EWHA_POST_GUBAK_LOG",
                            new List<string>() { "BP_IDX", "MEM_IDX", "L_DATE" });
                            var querySympathy = $"INSERT INTO `sympathy_jobs_part_time` (jobs_part_time_id, member_id, created_at, updated_at, is_dislike) VALUES ";
                            var dsQuerySympathy = GetQueryIn(dataProviderSympathy, querySympathy, 0, 3000, idlist, "BP_IDX", true);
                            dataProvider.ExcuteQuery(dsQuerySympathy);
                        }
                    }


                    /// Comment SYMPATHY
                    if (!string.IsNullOrEmpty(idlistComment))
                    {
                        try
                        {
                            DataProvider dataProviderSympathy = new DataProvider(
                                "EWHAIAN_LOG",
                                "REPLE_GOOD_LOG",
                                new List<string>() { "BR_IDX", "MEM_IDX", "L_DATE" });
                            var querySympathy = $"INSERT INTO `sympathy_jobs_comments` (comments_id, member_id, created_at, updated_at, is_dislike) VALUES ";
                            var dsQuerySympathy = GetQueryIn(dataProviderSympathy, querySympathy, 0, 3000, idlistComment, "BR_IDX", true);
                            dataProvider.ExcuteQuery(dsQuerySympathy);
                        }
                        catch
                        {

                        }
                        finally
                        {
                            DataProvider dataProviderSympathy = new DataProvider(
                            "EWHAIAN_LOG",
                            "REPLE_BAD_LOG",
                            new List<string>() { "BR_IDX", "MEM_IDX", "L_DATE" });
                            var querySympathy = $"INSERT INTO `sympathy_jobs_comments` (comments_id, member_id, created_at, updated_at, is_dislike) VALUES ";
                            var dsQuerySympathy = GetQueryIn(dataProviderSympathy, querySympathy, 0, 3000, idlistComment, "BR_IDX", true);
                            dataProvider.ExcuteQuery(dsQuerySympathy);
                        }
                    }
                    GC.Collect();
                }
            }

            Console.WriteLine("Done!!");
        }

        #endregion

        #region BP_IDX = 2
        /// <summary>
        /// BM_IDX = 2
        /// Move to jobs_part_time
        /// Count 60210
        /// </summary>
        public static void JobsPartTime()
        {
            DataProvider dataProvider = new DataProvider(
                "EWHAIAN",
                "EWHA_BOARD_CATEGORY",
                      new List<string>() { "CT_IDX", "CT_NAME", "CT_COLOR" });
            var query = $"INSERT INTO `shelter_categories` (id, name, background, color) VALUES ";
            var dsQuery = GetQuery(dataProvider, query, 0, 200, 17);
            dataProvider.ExcuteQuery(dsQuery);

            Console.WriteLine("Done!!");
        }

        #endregion

        #region BP_IDX = 13
        /// <summary>
        /// BM_IDX = 13
        /// Move to contents
        /// Count 60210
        /// </summary>
        public static void Contents()
        {
            DataProvider dataProvider = new DataProvider(
                "EWHAIAN",
                "EWHA_BOARD_POST",
                      new List<string>() { "BP_TITLE", "BP_CONTENT", "BP_COUNT", "MEM_IDX", "BP_DATE" });
            var query = $"INSERT INTO `contents` (title, content, lookup, member_id, created_at, updated_at, categories_contents_id) VALUES ";
            var dsQuery = GetQuery(dataProvider, query, 0, 0, 13);
            dataProvider.ExcuteQuery(dsQuery);

            Console.WriteLine("Done!!");
        }

        #endregion

        #region

        public static void Evaluation()
        {

        }

        #endregion

        /// <summary>
        /// </summary>
        /// <param name="dataProvider"></param>
        /// <returns></returns>
        public static List<Query> GetQuery(DataProvider dataProvider, string query, int from, int to, int idx)
        {
            var ds = new List<Query>();
            var idList = "";
            try
            {
                if (to >= 200)
                {
                    for (int i = from; i < to; i += 100)
                    {
                        var queryTemp = query;
                        var result = dataProvider.GetAllData("BM_IDX", i == 0 ? 0 : i + 1, 100, idx);
                        idList = result[result.Count - 1];
                        result.RemoveAt(result.Count - 1);
                        queryTemp += String.Join("," + Environment.NewLine, result);
                        if (!string.IsNullOrEmpty(queryTemp))
                        {
                            ds.Add(new Query()
                            {
                                Line = $"Line {i} to {i + 100}",
                                QueryString = queryTemp
                            });
                        }
                    }
                }
                else
                {
                    var result = dataProvider.GetAllData("BM_IDX", from, to, idx);
                    idList = result[result.Count - 1];
                    result.RemoveAt(result.Count - 1);
                    query += String.Join(", " + Environment.NewLine, result);
                    if (!string.IsNullOrEmpty(query))
                    {
                        ds.Add(new Query()
                        {
                            Line = $"Line {from} to {to}",
                            QueryString = query
                        });
                    }
                }
            }
            catch(Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error line {from} to {to}");
            }
            finally
            {
                dataProvider.CloseConnect();
            }
            ds.Add(new Query()
            {
                QueryString = idList,
                Line = null
            });
            return ds;
        }


        public static List<Query> GetQueryIn(DataProvider dataProvider, string query, int from, int to, string idx, string name, bool isCom)
        {
            var ds = new List<Query>();
            var idList = "";

            try
            {
                if (!isCom) // to >= 200 && 
                {
                    for (int i = from; i < to; i += 300)
                    {
                        var queryTemp = query;
                        var result = dataProvider.WhereIn(!isCom ? "XXX" : "BP_IDX", i == 0 ? 0 : i + 1, 300, idx, name, isCom);
                        if (result.Count > 1)
                        {
                            idList = result[result.Count - 1];
                            result.RemoveAt(result.Count - 1);
                        }
                        if (result.Count > 0)
                        {
                            queryTemp += String.Join("," + Environment.NewLine, result);
                            if (!string.IsNullOrEmpty(queryTemp))
                            {
                                ds.Add(new Query()
                                {
                                    Line = $"Line {i} to {i + 300}",
                                    QueryString = queryTemp
                                });
                            }
                        }
                    }
                }
                else
                {
                    var result = dataProvider.WhereIn(!isCom ? "BR_IDX" : "BP_IDX", from, to, idx, name, isCom);
                    if (result.Count > 1)
                    {
                        idList = result[result.Count - 1];
                        result.RemoveAt(result.Count - 1);
                    }
                    if (result.Count > 0)
                    {
                        query += String.Join(", " + Environment.NewLine, result);
                        if (!string.IsNullOrEmpty(query))
                        {
                            ds.Add(new Query()
                            {
                                Line = $"Line {from} to {to}",
                                QueryString = query
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error line {from} to {to}");
            }
            finally
            {
                dataProvider.CloseConnect();
            }
            ds.Add(new Query()
            {
                QueryString = idList,
                Line = null
            });
            return ds;
        }
    }

}
