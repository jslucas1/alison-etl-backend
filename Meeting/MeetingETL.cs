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

        public void DoWork(Database db)
        {
            Console.WriteLine("In the Meeting do work");
            List<Meeting> meetings = GetAllFromDB(db.ConnString);
            Console.WriteLine($"{meetings.Count} Meetings Loaded");

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

        private List<Meeting> GetAllFromDB(string connString)
        {
            List<Meeting> meetings = new List<Meeting>();
            using var con = new MySqlConnection(connString);

            string stm = "select * from `alison`.HearingsMeetings";

            try
            {
                con.Open();
                using var cmd2 = new MySqlCommand(stm, con);
                using (var rdr = cmd2.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        meetings.Add(new Meeting()
                        {
                            Id = rdr.GetInt32(0),
                            Body = rdr.GetString(1),
                            EventType = rdr.GetString(2),
                            EventDate = rdr.GetString(3),
                            EventTime = rdr.GetString(4),
                            DeadlineDate = rdr.GetString(5),
                            DeadlineTime = rdr.GetString(6),
                            Location = rdr.GetString(7),
                            EventTitle = rdr.GetString(8),
                            EventDescription = rdr.GetString(9)
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                con.Close();
            }
            return meetings;
        }
    }
}