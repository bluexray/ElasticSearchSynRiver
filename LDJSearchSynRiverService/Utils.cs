using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web;
    public class Utils
    {
        public static string RootPath
        {
            get
            {
                string strAssemblyFilePath = Assembly.GetExecutingAssembly().Location;
                string strAssemblyDirPath = Path.GetDirectoryName(strAssemblyFilePath);
                return strAssemblyDirPath;
            }
        }
        private static object _locker = new object();//锁对象
        public static void WriteLogFile(string input)
        {
            lock (_locker)
            {
                FileStream fs = null;
                StreamWriter sw = null;
                try
                {
                    string fileName = RootPath + "\\logs\\" + DateTime.Now.ToString("yyyyMMdd") + ".log";

                    FileInfo fileInfo = new FileInfo(fileName);
                    if (!fileInfo.Directory.Exists)
                    {
                        fileInfo.Directory.Create();
                    }
                    if (!fileInfo.Exists)
                    {
                        fileInfo.Create().Close();
                    }
                    else if (fileInfo.Length > 2048 * 1000)
                    {
                        fileInfo.Delete();
                    }

                    fs = fileInfo.OpenWrite();
                    sw = new StreamWriter(fs);
                    sw.BaseStream.Seek(0, SeekOrigin.End);
                    sw.Write("Log Entry : ");
                    sw.Write("{0}", DateTime.Now.ToString("yyyy年MM月dd日 HH:mm:ss"));
                    sw.Write(Environment.NewLine);
                    sw.Write(input);
                    sw.Write(Environment.NewLine);
                    sw.Write("------------------------------------");
                    sw.Write(Environment.NewLine);
                }
                catch (Exception ex)
                {
                    //throw ex;
                }
                finally
                {
                    if (sw != null)
                    {
                        sw.Flush();
                        sw.Close();
                        sw.Dispose();
                    }
                    if (fs != null)
                    {
                        fs.Close();
                        fs.Dispose();
                    }
                }
            }
        }
    }
