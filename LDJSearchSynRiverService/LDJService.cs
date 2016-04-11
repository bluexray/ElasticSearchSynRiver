using ElasticSearchSynSchedule.Common;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;

namespace LDJWindowsService
{
    public partial class LDJService : ServiceBase
    {
        public LDJService()
        {
            InitializeComponent();
        }
        System.Threading.Timer timer;
        protected override void OnStart(string[] args)
        {
            
            try
            {
                Utils.WriteLogFile("启动服务...");
                string time = ConfigurationManager.AppSettings["time"].ToString();
                DateTime lastruntime = DateTime.Parse("1900-01-01");
                timer = new Timer((o) =>
                {
                    if (DateTime.Now - lastruntime > TimeSpan.FromMinutes(int.Parse(time)))
                    {
                        lastruntime = DateTime.Now;
                        try
                        {
                            Utils.WriteLogFile("正在导入数据...");
                            string rs = ElasticSearchHelper.SetIndexAlias();
                            if (rs.Length > 0)
                            {
                                Utils.WriteLogFile("导入完成，新的索引名称为：" + rs);
                            }
                            else
                            {
                                Utils.WriteLogFile("导入失败");
                            }
                        }
                        catch (Exception ex)
                        {
                            Utils.WriteLogFile(ex.Message);
                        }
                    }
                }, null, 0, 1000);
                Utils.WriteLogFile("服务已启动");
            }
            catch (Exception ex1)
            {
                Utils.WriteLogFile(ex1.Message);
            }
        }

        protected override void OnStop()
        {
            Utils.WriteLogFile("服务已停止");
        }
    }
}
