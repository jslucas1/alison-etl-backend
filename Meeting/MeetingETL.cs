using etl.Interfaces;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace etl.Meeting
{
    public class MeetingETL : IEtl
    {
        private int number = 0;
        private Database db;

        public MeetingETL()
        {
            db = new Database();
        }

        public void DoWork()
        {
            Console.WriteLine("In the Meeting do work");

        }

        public bool ShouldRun()
        {
            number += 1;

            if (number % 5 == 0)
            {
                return true;
            }

            return false;
        }

        public List<ExpandoObject> GetAllFromDB()
        {
            throw new NotImplementedException();
        }

        public List<ExpandoObject> GetLinxData()
        {
            throw new NotImplementedException();
        }

        public void LoadLinxTable(List<ExpandoObject> linxData)
        {
            throw new NotImplementedException();
        }

        public void InsertData(string insert_proc_name)
        {
            throw new NotImplementedException();
        }

        public void UpdateData(string update_proc_name)
        {
            throw new NotImplementedException();
        }

        public void DeleteData(string delete_proc_name)
        {
            throw new NotImplementedException();
        }
    }
}