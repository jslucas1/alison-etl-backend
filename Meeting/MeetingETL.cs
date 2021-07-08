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
        private int number = 0;

        public void DoWork()
        {
            Console.WriteLine($"Run Meeting update logic here");
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