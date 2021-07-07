using etl.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace etl.Meeting
{
    public class MeetingETL : IEtl
    {
        // Replace this with sql call for last run info
        private int number = 0;

        // do the work for the update
        public void DoWork()
        {
            Console.WriteLine($"Run Meeting update logic here");
        }

        // is it time to run calculation base off last run data
        public bool ShouldRun()
        {
            number += 1;

            // fake calc for "time to run" example
            if (number % 5 == 0)
            {
                return true;
            }

            return false;
        }
    }
}