using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using ElasticSearchSynSchedule.Common;
using Newtonsoft.Json;

namespace ElasticSearchSynSchedule
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            try
            {
                string IndexNew = string.Format("{0:yyyyMMddHHmmss}", DateTime.Now);
                Console.WriteLine("正在导入数据...");
                string rs = ElasticSearchHelper.SetIndexAlias();
                //ElasticSearchHelper.CreateIndex();
                //Console.WriteLine(IndexNew);

                //ElasticSearchHelper.ImportDate();


                //            string xml =
                //                @"<root><type>jdbc</type><jdbc><url>jdbc:sqlserver://localhost:1433;databaseName=ldjmall</url><user>sa</user><password>35274875</password><sql>SELECT * FROM bma_products</sql>
                //<index>20150808221024</index><type>mytype</type></jdbc></root>";

                //            XmlDocument doc = new XmlDocument();
                //            doc.LoadXml(xml);
                //            string json = Newtonsoft.Json.JsonConvert.SerializeXmlNode(doc);

                //            var len = json.Length;

                //            json = json.Substring(8, len - 10);

                //            Console.WriteLine("XML -> JSON: {0}", json);
                if (rs.Length > 0)
                {
                    Console.WriteLine("导入完成，新的索引名称为：" + rs);
                }
                else
                {
                    Console.WriteLine("导入失败");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            int count = 100;
            for (int i = 0; i < count; i++)
            {
                System.Threading.Thread.Sleep(1000);
                Console.WriteLine((count - i) + "秒后自动关闭");
            }
            
        }
    }
}