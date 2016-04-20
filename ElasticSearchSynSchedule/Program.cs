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
                if (rs.Length > 0)
                {
                    Utils.WriteLogFile("Mall导入完成，新的索引名称为：" + rs);
                    Console.WriteLine("Mall导入完成，新的索引名称为：" + rs);
                }
                else
                {
                    Utils.WriteLogFile("Mall导入失败");
                    Console.WriteLine("Mall导入失败");
                }

                rs = ElasticSearchHelper.SetSuggestIndex();
                if (rs.Length > 0)
                {
                    Utils.WriteLogFile("MallSuggest导入完成，新的索引名称为：" + rs);
                    Console.WriteLine("MallSuggest导入完成，新的索引名称为：" + rs);
                }
                else
                {
                    Utils.WriteLogFile("MallSuggest导入失败");
                    Console.WriteLine("MallSuggest导入失败");
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