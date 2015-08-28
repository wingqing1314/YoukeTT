using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WCFService.DAL
{
    public class DailyDAL
    {
        private static DailyDAL Instance;
        private DailyDAL() { }

        public static DailyDAL GetInstance()
        {
            if (Instance == null)
                Instance = new DailyDAL();
            return Instance;
        }
    }
}