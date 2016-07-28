using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Net;
using System.Net.Mime;
using System.Xml;


using Nest;
using Elasticsearch.Net;
using ServiceStack;
using Newtonsoft.Json;
using Dapper.Wrapper;


namespace ElasticSearchSynSchedule.Common
{
    public static class ElasticSearchHelper
    {
        private static string Host = ConfigurationManager.AppSettings["SearchUrl"].ToString();

        private static ElasticClient GetClient(string indexname)
        {
            var node = new Uri(Host);

            var settings = new ConnectionSettings(
                node,
                defaultIndex: indexname
            );

            return new ElasticClient(settings);
        }

        public static string SetIndexAlias()
        {
            string IndexAlias = ConfigurationManager.AppSettings["IndexAlias"].ToString();
            string IndexNew = string.Format("{0:yyyyMMddHHmmss}mall", DateTime.Now);
            var clinet = GetClient(IndexNew);

            //创建索引
            var qc = clinet.CreateIndex(s => s
                             .Index(IndexNew)
                             .Analysis(f => f.Analyzers(qs => qs.Add("default", new LanguageAnalyzer(Language.ik))))
                             );
            ////设置索引map
            //if (qc.Acknowledged)
            //{
            //    var response1 = clinet.Map<object>(m => m.Index(IndexNew).Type("product").Properties(a => a.Completion(b => b.Name("keywords").Payloads(true).PreserveSeparators(false).PreservePositionIncrements(false).MaxInputLength(50).IndexAnalyzer("ik"))));

            //}

            //根据索引别名获取旧索引的名称
            string IndexOld = "";
            var result = clinet.GetAlias(c => c.Alias(IndexAlias));
            if (result.Indices.Count > 0)
            {
                //get default old indexname
                IndexOld = result.Indices.First().Key;
            }

            //river导入数据
            if (!ImportMallDataBulk(IndexNew))
            {
                clinet.DeleteIndex(IndexNew);
                return "";
            }
                

            //移除旧索引的别名
            clinet.Alias(m => m
                .Remove(f => f
                    .Index(IndexOld)
                    .Alias(IndexAlias)));


            //增加新索引的别名
            clinet.Alias(m => m
                .Add(q => q
                    .Index(IndexNew)
                    .Alias(IndexAlias)));

            //删除旧的索引
            if (IndexOld != null && IndexOld != "")
            {
                clinet.DeleteIndex(IndexOld);
            }
            //返回新索引名称
            return IndexNew;
        }

        public static string SetSuggestIndex()
        {
            try
            {
                string IndexName = string.Format("{0:yyyyMMddHHmmss}suggest", DateTime.Now);
                string SuggestIndexAlias = ConfigurationManager.AppSettings["SuggestIndexAlias"].ToString();// "mallsuggest";
                string SuggestIndexOld = "";
                var clinet = GetClient(IndexName);

                //创建索引
                var qc = clinet.CreateIndex(s => s
                                 .Index(IndexName)
                                 .Analysis(f => f.Analyzers(qs => qs.Add("default", new LanguageAnalyzer(Language.ik))))
                                 );
                //设置索引map
                if (qc.Acknowledged)
                {
                    var response1 = clinet.Map<object>(m => m.Index(IndexName).Properties(a => a.Completion(b => b.Name("keywords").Payloads(true).PreserveSeparators(false).PreservePositionIncrements(false).MaxInputLength(50))));
                }
                else
                {
                    Utils.WriteLogFile("搜索建议索引创建失败");
                }

                //根据索引别名获取旧索引的名称
                var result = clinet.GetAlias(c => c.Alias(SuggestIndexAlias));
                if (result.Indices.Count > 0)
                {
                    //get default old indexname
                    SuggestIndexOld = result.Indices.First().Key;
                }

                #region 导入数据
                bool rs1 = ImportBrandSuggestDataBulk(IndexName);
                bool rs2 = ImportStoreSuggestDataBulk(IndexName);
                if (!rs1||!rs2)
                {
                    Utils.WriteLogFile("导入数据失败");
                    clinet.DeleteIndex(IndexName);
                    return "";
                }
                #endregion

                //移除旧索引的别名
                clinet.Alias(m => m
                    .Remove(f => f
                        .Index(SuggestIndexOld)
                        .Alias(SuggestIndexAlias)));


                //增加新索引的别名
                clinet.Alias(m => m
                    .Add(q => q
                        .Index(IndexName)
                        .Alias(SuggestIndexAlias)));

                //删除旧的索引
                if (SuggestIndexOld != null && SuggestIndexOld != "")
                {
                    clinet.DeleteIndex(SuggestIndexOld);
                }

                //返回新索引名称
                return IndexName;
            }
            catch (Exception ex)
            {
                return "error:" + ex.Message;
            }
        }

        public static bool ImportBrandSuggestDataBulk(string IndexNew)
        {
            int PageSize = int.Parse(ConfigurationManager.AppSettings["PageSize"].ToString());
            int PageNumber = 1;
            var clinet = GetClient(IndexNew);
            List<int> rs = new List<int>();
            Utils.WriteLogFile("ImportBrandSuggestDataBulk开始导入数据(" + IndexNew + ")页大小" + PageSize);
            while (true)
            {
                SQLinq.Dynamic.DynamicSQLinq sql = new SQLinq.Dynamic.DynamicSQLinq("v_ProductInfo_s ");
                sql = sql.Select("brandid,BrandName name").GroupBy("brandid,BrandName");
                sql = sql.Skip(PageSize * (PageNumber - 1)).Take(PageSize).OrderByDescending("brandid");
                var list = db.Query<dynamic>(sql);
                if (list.Count == 0)
                {
                    break;
                }
                List<object> keywordslist = new List<object>();
                string initials = "";
                string pinyin = "";
                string output = "";

                SQLinq.Dynamic.DynamicSQLinq sqlkeywords = new SQLinq.Dynamic.DynamicSQLinq("bma_brands ");
                sqlkeywords = sqlkeywords.Select("brandid,keywords");
                var listkeywords = db.Query<dynamic>(sqlkeywords).ToList();

                foreach (var item in list)
                {
                    output = item.name;
                    initials = "";
                    pinyin = ChineseToPinYin.ToPinYin(output, ref initials);
                    keywordslist.Add(new { keywords = new { input = new List<string>() { output, pinyin, initials }, output = output, payload = new { id = item.brandid.ToString(), type = "brand", keyword = "" }, weight = 20 } });
                    
                    //添加关键词
                    var brandinfo = listkeywords.FirstOrDefault(a => a.brandid == item.brandid);
                    if (brandinfo != null && brandinfo.keywords != null)
                    {
                        List<string> brandkeywords = ((string)brandinfo.keywords).Split(',').ToList();
                        foreach (var key in brandkeywords)
                        {
                            output = item.name + key;
                            initials = "";
                            pinyin = ChineseToPinYin.ToPinYin(output, ref initials);
                            keywordslist.Add(new { keywords = new { input = new List<string>() { output, pinyin, initials }, output = output, payload = new { id = item.brandid.ToString(), type = "brand", keyword = key }, weight = 20 } });
                        }
                    }
                }
                var bk = clinet.Bulk(a => a.IndexMany<object>(keywordslist));
                if (bk.Errors)
                {
                    Utils.WriteLogFile("（页码" + PageNumber + "数据量" + list.Count + "）(" + IndexNew + ")导入有错误，数据来源sql语句--" + sql.ToSQL().ToQuery());
                    rs.Add(0);
                }
                else
                {
                    Utils.WriteLogFile("（页码" + PageNumber + "数据量" + list.Count + "）(" + IndexNew + ")导入成功");
                    rs.Add(1);
                }
                PageNumber++;
            }
            Utils.WriteLogFile("ImportBrandSuggestDataBulk导入数据结束(" + IndexNew + ")");
            return rs.Where(a => a == 0).Count() == 0;
        }
        public static bool ImportStoreSuggestDataBulk(string IndexNew)
        {
            int PageSize = int.Parse(ConfigurationManager.AppSettings["PageSize"].ToString());
            int PageNumber = 1;
            var clinet = GetClient(IndexNew);
            List<int> rs = new List<int>();
            Utils.WriteLogFile("ImportStoreSuggestDataBulk开始导入数据(" + IndexNew + ")页大小" + PageSize);
            while (true)
            {
                SQLinq.Dynamic.DynamicSQLinq sql = new SQLinq.Dynamic.DynamicSQLinq("v_ProductInfo_s ");
                sql = sql.Select("storeid,StoreName name").GroupBy("storeid,StoreName");
                sql = sql.Skip(PageSize * (PageNumber - 1)).Take(PageSize).OrderByDescending("storeid");
                var list = db.Query<dynamic>(sql);
                if (list.Count == 0)
                {
                    break;
                }
                string initials = "";
                var keywordslist = list.Select(a => new { keywords = new { input = new List<string>() { a.name, ChineseToPinYin.ToPinYin(a.name, ref initials), initials }, output = a.name, payload = new { id = a.storeid.ToString(), type = "store", keyword = "" }, weight = 10 } });
                var bk = clinet.Bulk(a => a.IndexMany<object>(keywordslist));
                if (bk.Errors)
                {
                    Utils.WriteLogFile("（页码" + PageNumber + "数据量" + list.Count + "）(" + IndexNew + ")导入有错误，数据来源sql语句--" + sql.ToSQL().ToQuery());
                    rs.Add(0);
                }
                else
                {
                    Utils.WriteLogFile("（页码" + PageNumber + "数据量" + list.Count + "）(" + IndexNew + ")导入成功");
                    rs.Add(1);
                }
                PageNumber++;
            }
            Utils.WriteLogFile("ImportStoreSuggestDataBulk导入数据结束(" + IndexNew + ")");
            return rs.Where(a => a == 0).Count() == 0;
        }

        public static bool ImportMallDataBulk(string IndexNew)
        {
            int PageSize = int.Parse(ConfigurationManager.AppSettings["PageSize"].ToString());
            int PageNumber = 1;
            var clinet = GetClient(IndexNew);
            List<int> rs = new List<int>();
            Utils.WriteLogFile("ImportMallDataBulk开始导入数据(" + IndexNew + ")页大小" + PageSize);
            while (true)
            {
                SQLinq.Dynamic.DynamicSQLinq sql = new SQLinq.Dynamic.DynamicSQLinq("v_ProductInfo_s ");
                sql = sql.Select("*");
                sql = sql.Skip(PageSize * (PageNumber - 1)).Take(PageSize).OrderByDescending("pid");
                var list = db.Query<dynamic>(sql);
                if (list.Count == 0)
                {
                    break;
                }
                var bk = clinet.Bulk(a => a.IndexMany(list));
                if (bk.Errors)
                {
                    Utils.WriteLogFile("（页码" + PageNumber + "数据量" + list.Count + "）(" + IndexNew + ")导入有错误，数据来源sql语句--" + sql.ToSQL().ToQuery());
                    rs.Add(0);
                }
                else
                {
                    Utils.WriteLogFile("（页码" + PageNumber + "数据量" + list.Count + "）(" + IndexNew + ")导入成功");
                    rs.Add(1);
                }
                PageNumber++;
            }
            Utils.WriteLogFile("ImportMallDataBulk导入数据结束(" + IndexNew + ")");
            return rs.Where(a => a == 0).Count() == 0;
        }
        public static DBContext db 
        {
            get
            {
                return new DBContext("ldjmallconstr");
            } 
        }

        //public static bool ImportDate(string IndexNew)
        //{

        //    string path = AppDomain.CurrentDomain.BaseDirectory + @"\river\mssql.xml";
        //    string json = string.Empty;


        //    string url = Host + "/_river/"+IndexNew+"/_meta";

        //    try
        //    {
        //        var doc = new XmlDocument();
        //        doc.Load(path);

        //        XmlNode node = doc.SelectSingleNode("/mssql/jdbc/index");

        //        XmlElement xe = (XmlElement)node;

        //        xe.InnerText = IndexNew;

        //        doc.Save(path);

        //        json = JsonConvert.SerializeXmlNode(doc);


        //        // var j = JsonConvert.SerializeXmlNode(doc.SelectNodes("/mssql")[0]);

        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
        //    }

        //    var len = json.Length;

        //    json = json.Substring(9, len - 10);


        //    var client = new JsonServiceClient(url);
        //    WebClient web = new WebClient();


        //    var c = new JsonHttpClient(url);



        //  var result =  web.UploadString(url, json);
        //    //client.Put(new {type = "jdbc"});
        //    //var result = client.Put( new string { Text = json });

        //    //if (result.StatusCode==HttpStatusCode.OK)
        //    //{
        //    //    return true;
        //    //}
        //  //{"_index":"_river","_type":"20160330202029","_id":"_meta","_version":1,"created":true}
        //  var rs = Newtonsoft.Json.JsonConvert.DeserializeObject<Newtonsoft.Json.Linq.JObject>(result);
        //  bool a= rs.Value<bool>("created");
        //  return a;
        //}
    }
}
