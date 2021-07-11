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

    }
}