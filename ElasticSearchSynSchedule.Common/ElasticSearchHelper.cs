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


namespace ElasticSearchSynSchedule.Common
{
    public static class ElasticSearchHelper
    {
        private static string IndexOld = string.Empty;

        private static string IndexNew = "";

        private static string IndexAlias = ConfigurationManager.AppSettings["IndexAlias"].ToString();

        private static string Host = ConfigurationManager.AppSettings["SearchUrl"].ToString();

        private const string IndexName = "my-application";


        private static ElasticClient GetClient()
        {
            var node = new Uri(Host);

            var settings = new ConnectionSettings(
                node,
                defaultIndex: IndexName
            );

            return new ElasticClient(settings);
        }

        public static string SetIndexAlias()
        {
            IndexNew = string.Format("{0:yyyyMMddHHmmss}", DateTime.Now);
            var clinet = GetClient();


            var qc = clinet.CreateIndex(s => s
                             .Index(IndexNew)
                             .Analysis(f => f.Analyzers(qs => qs.Add("default", new LanguageAnalyzer(Language.ik))))
                             );

            var result = clinet.GetAlias(c => c.Alias(IndexAlias));
            if (result.Indices.Count > 0)
            {
                //get default old indexname
                IndexOld = result.Indices.First().Key;  
            }


            if (!ImportDate()) return "";
            clinet.Alias(m => m
                .Remove(f => f
                    .Index(IndexOld)
                    .Alias(IndexAlias)));


            //set new index  alais
            clinet.Alias(m => m
                .Add(q => q
                    .Index(IndexNew)
                    .Alias(IndexAlias)));
            if (IndexOld != null && IndexOld != "")
            {
                clinet.DeleteIndex(IndexOld);
            }
            return IndexNew;
        }


        public static bool ImportDate()
        {

            string path = AppDomain.CurrentDomain.BaseDirectory + @"\river\mssql.xml";
            string json = string.Empty;


            string url = Host + "/_river/"+IndexNew+"/_meta";

            try
            {
                var doc = new XmlDocument();
                doc.Load(path);

                XmlNode node = doc.SelectSingleNode("/mssql/jdbc/index");

                XmlElement xe = (XmlElement)node;

                xe.InnerText = IndexNew;

                doc.Save(path);

                json = JsonConvert.SerializeXmlNode(doc);


                // var j = JsonConvert.SerializeXmlNode(doc.SelectNodes("/mssql")[0]);

            }
            catch (Exception ex)
            {
                
                throw;
            }

            var len = json.Length;

            json = json.Substring(9, len - 10);


            var client = new JsonServiceClient(url);

            WebClient web = new WebClient();


            var c = new JsonHttpClient(url);

            

          var result =  web.UploadString(url, json);
            //client.Put(new {type = "jdbc"});
            //var result = client.Put( new string { Text = json });

            //if (result.StatusCode==HttpStatusCode.OK)
            //{
            //    return true;
            //}
          //{"_index":"_river","_type":"20160330202029","_id":"_meta","_version":1,"created":true}
          var rs = Newtonsoft.Json.JsonConvert.DeserializeObject<Newtonsoft.Json.Linq.JObject>(result);
          bool a= rs.Value<bool>("created");
          return a;
        }
    }
}
