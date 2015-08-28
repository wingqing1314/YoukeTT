using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Microsoft.Practices.EnterpriseLibrary.Caching;
using Microsoft.Practices.EnterpriseLibrary.Caching.Expirations;
using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Logging;
using Microsoft.Practices.EnterpriseLibrary.Logging.ExtraInformation;
using Microsoft.Practices.EnterpriseLibrary.Logging.Filters;

namespace WCFService.SQLHelper
{
    public class ELDbHelper
    {
        private string _DBName;
        private int _CacheTimeOut;

        private ICacheManager _DataCacheManager = null;

        public ELDbHelper()
        {
            //
            // TODO: 在此处添加构造函数逻辑
            //
            _DBName = ConfigurationManager.AppSettings["DBName"];
        }

        //public string DBName
        //{
        //    get { return _DBName; }
        //    set { _DBName = value; }
        //}

        /// <summary>
        /// 缓存过期时间,以秒作单位
        /// </summary>
        public int CacheTimeOut
        {
            get { return _CacheTimeOut; }
            set { _CacheTimeOut = value; }
        }

        /// <summary>
        /// 返回链接数据库的字符串
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            Database db = DatabaseFactory.CreateDatabase(_DBName);
            return db.ConnectionString;
        }

        /// <summary>
        /// 返回SQL语句首行首列的值
        /// </summary>
        /// <param name="SQL">SQL 语句</param>
        /// <returns></returns>
        public string GetScalar(string SQL)
        {
            // connect to database
            Database db = DatabaseFactory.CreateDatabase(_DBName);
            // record the sql for debug
            LogInfo(SQL, "GetCountBySql");
            // run the sql
            DbCommand dbCommand = db.GetSqlStringCommand(SQL);
            string cReturn = db.ExecuteScalar(dbCommand).ToString();

            return cReturn;
        }

        /// <summary>
        /// 运行 一个 SQL 语句,返回 datareader
        /// </summary>
        /// <param name="SQL">SQL 语句</param>
        /// <returns></returns>
        public IDataReader GetDataReader(string SQL)
        {
            // connect to database
            Database db = DatabaseFactory.CreateDatabase(_DBName);

            // record the sql for debug
            //LogInfo(SQL, "GetDataReader");

            // run the sql
            DbCommand dbCommand = db.GetSqlStringCommand(SQL);
            IDataReader oDR = db.ExecuteReader(dbCommand);

            return oDR;
        }

        /// <summary>
        /// 运行 一个 SQL 语句或存储过程,返回 dataset
        /// </summary>
        /// <param name="SQL">SQL 语句</param>
        /// <returns></returns>
        public DataSet GetDataSet(string SQL)
        {
            // connect to database
            Database db = DatabaseFactory.CreateDatabase(_DBName);

            // record the sql for debug
            //LogInfo(SQL, "GetDataSet");

            // run the sql
            DbCommand dbCommand = db.GetSqlStringCommand(SQL);
            DataSet oDS = db.ExecuteDataSet(dbCommand);

            return oDS;
        }

        /// <summary>
        /// 运行 一个 SQL 语句, 如 insert, update, delete 返回影响行数
        /// </summary>
        /// <param name="SQL">SQL 语句</param>
        /// <returns></returns>
        public int ExecuteSql(string SQL)
        {
            // connect to database
            Database db = DatabaseFactory.CreateDatabase(_DBName);

            // record the sql for debug
            //LogInfo(SQL, "ExecuteSql");

            // run the sql
            DbCommand dbCommand = db.GetSqlStringCommand(SQL);
            int nReturn = db.ExecuteNonQuery(dbCommand);

            return nReturn;
        }

        /// <summary>
        /// 运行 一个 SQL 语句或存储过程, 指定超时时间, 返回影响行数
        /// </summary>
        /// <param name="SQL">SQL 语句或存储过程</param>
        /// <returns></returns>
        public int ExecuteSql(string SQL, int nTimeout)
        {
            // connect to database
            Database db = DatabaseFactory.CreateDatabase(_DBName);

            // record the sql for debug
            //LogInfo(SQL, "ExecuteSql_Timeout");

            // run the sql
            DbCommand dbCommand = db.GetSqlStringCommand(SQL);
            dbCommand.CommandTimeout = nTimeout;
            int nReturn = db.ExecuteNonQuery(dbCommand);

            return nReturn;
        }

        /// <summary>
        /// 写日志
        /// </summary>
        /// <param name="cMsg"></param>
        public void LogInfo(string cMsg)
        {
            LogEntry log = new LogEntry();
            log.EventId = 100;
            log.Message = cMsg;
            log.Priority = -1;
            log.TimeStamp = DateTime.Now;
            log.Title = "Info";

            Logger.Write(log);
        }

        /// <summary>
        /// 写日志
        /// </summary>
        /// <param name="cMsg"></param>
        /// <param name="cTitle"></param>
        public void LogInfo(string cMsg, string cTitle)
        {
            LogEntry log = new LogEntry();
            log.EventId = 100;
            log.Message = cMsg;
            log.Priority = -1;
            log.Title = cTitle;
            log.TimeStamp = DateTime.Now;

            Logger.Write(log);
        }

        /// <summary>
        /// 设置缓存
        /// by Hades.Gao
        /// </summary>
        /// <param name="cCacheManager"></param>
        public void SetCacheManager(string cCacheManager)
        {
            try
            {
                if (String.IsNullOrEmpty(cCacheManager))
                    _DataCacheManager = CacheFactory.GetCacheManager();
                else
                    _DataCacheManager = CacheFactory.GetCacheManager(cCacheManager);
            }
            catch (Exception ex)
            {
                this.LogInfo("配置缓存时出错:" + ex.Message + "\r\nManagerName:" + cCacheManager);
            }
        }

        /// <summary>
        /// 添加缓存
        /// by Hades.Gao
        /// </summary>
        /// <param name="cKey">缓存关键字</param>
        /// <param name="obj">缓存对象</param>
        public void AddCache(string cKey, object obj)
        {
            AddCache(cKey, obj, _CacheTimeOut);
        }

        /// <summary>
        /// 添加缓存
        /// by Hades.Gao
        /// </summary>
        /// <param name="cKey">缓存关键字</param>
        /// <param name="obj">缓存对象</param>
        /// <param name="_CacheTimeOut">过期时间,以秒作单位</param>
        public void AddCache(string cKey, object obj, int _CacheTimeOut)
        {
            try
            {

                TimeSpan refreshTime = new TimeSpan(0, 0, _CacheTimeOut);
                AbsoluteTime expireTime = new AbsoluteTime(refreshTime);

                if (!Object.Equals(_DataCacheManager, null))
                    _DataCacheManager.Add(cKey, obj, CacheItemPriority.Normal, null, expireTime);

            }
            catch (Exception ex)
            {
                this.LogInfo("添加缓存时出错:" + ex.Message + "\r\nTargetSite:" + ex.TargetSite + "\r\nKey:" + cKey + "\r\nObjectType:" + obj.GetType() + "\r\nObject:" + obj.ToString() + "\r\nTimeOut:" + _CacheTimeOut.ToString());
            }
        }

        /// <summary>
        /// 读取缓存
        /// by Hades.Gao
        /// </summary>
        /// <param name="cKey">缓存关键字</param>
        /// <returns></returns>
        public object GetCache(string cKey)
        {
            object obj = null;
            try
            {
                if (!Object.Equals(_DataCacheManager, null))
                    obj = _DataCacheManager.GetData(cKey);
            }
            catch (Exception ex)
            {
                this.LogInfo("读取缓存时出错:" + ex.Message);
            }
            return obj;
        }

        /// <summary>
        /// 移除缓存
        /// by Hades.Gao
        /// </summary>
        /// <param name="cKey">缓存关键字</param>
        /// <returns></returns>
        public void RemoveCache(string cKey)
        {
            try
            {
                if (!Object.Equals(_DataCacheManager, null))
                    _DataCacheManager.Remove(cKey);
            }
            catch (Exception ex)
            {
                this.LogInfo("移除缓存时出错:" + ex.Message);
            }
        }

        /// <summary>
        /// 调用存储过程返回list
        /// </summary>
        /// <param name="Form">分类规则</param>
        /// <param name="IsPagination">是否分页</param>
        /// <param name="where">条件</param>
        /// <param name="PageSize">页大小</param>
        /// <param name="PageCurrent">第几页</param>
        /// <param name="ExVar">扩展参数</param>
        /// <param name="pageCount">总页数</param>
        /// <param name="Counts">总记录数</param>
        /// <returns></returns>
        public DataSet GetDataSet(string Form, int IsPagination, string where, int PageSize, int PageCurrent, string ExVar, ref int pageCount, ref int Counts)
        {
            Database db = DatabaseFactory.CreateDatabase(_DBName);
            DbCommand myCommand = db.GetStoredProcCommand("GetDataList");
            db.AddInParameter(myCommand, "@Form", DbType.String, Form);
            db.AddInParameter(myCommand, "@IsPagination", DbType.Int32, IsPagination);
            db.AddInParameter(myCommand, "@sqlWhere", DbType.String, where);
            db.AddInParameter(myCommand, "@pageSize", DbType.Int32, PageSize);
            db.AddInParameter(myCommand, "@page", DbType.Int32, PageCurrent);
            db.AddInParameter(myCommand, "@ExVar", DbType.String, ExVar);
            db.AddOutParameter(myCommand, "@pageCount", DbType.Int32, 4);
            db.AddOutParameter(myCommand, "@Counts", DbType.Int32, 4);
            DataSet oDS = db.ExecuteDataSet(myCommand);
            Object o = db.GetParameterValue(myCommand, "@pageCount");
            Object o1 = db.GetParameterValue(myCommand, "@Counts");
            if (o.ToString() != "")
                pageCount = int.Parse(o.ToString());
            if (o1.ToString() != "")
                Counts = int.Parse(o1.ToString());
            return oDS;
        }
    }
}