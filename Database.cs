using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;


namespace etl
{
    public class Database
    {
        public string ConnString { get; set; }

        // CTOR 
        public Database(string server, string name, string port, string username, string password)
        {
            this.ConnString = $@"server = {server};user={username};database={name};port={port};password={password};";
        }

        // CTOR loading from ENV
        public Database()
        {
            string server = Environment.GetEnvironmentVariable("alison_database_server");
            string name = Environment.GetEnvironmentVariable("alison_database_name");
            string port = Environment.GetEnvironmentVariable("alison_database_port");
            string username = Environment.GetEnvironmentVariable("alison_database_username");
            string password = Environment.GetEnvironmentVariable("alison_database_password");
            Console.WriteLine("got the datbase " + server);

            this.ConnString = $@"server = {server};user={username};database={name};port={port};password={password};";
        }

        //Generic Select Query Function
        public List<ExpandoObject> Select(string query)
        {
            List<ExpandoObject> results = new();
            using var con = new MySqlConnection(this.ConnString);
            try
            {
                con.Open();
                using var cmd2 = new MySqlCommand(query, con);
                using var rdr = cmd2.ExecuteReader();
                while (rdr.Read())
                {
                    var temp = new ExpandoObject() as IDictionary<string, Object>;
                    for (int i = 0; i < rdr.FieldCount; i++)
                    {
                        temp.TryAdd(rdr.GetName(i), rdr.GetValue(i));
                    }

                    results.Add((ExpandoObject)temp);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Select Query Error");
                Console.WriteLine(e.Message);
            }
            finally
            {
                con.Close();
            }

            return results;
        }

        public void Update() { }

        public void Dalete() { }

        public void StoredProc(string procName)
        {
            using var conn = new MySqlConnection(this.ConnString);
            try
            {
                conn.Open();
                using var cmd = new MySqlCommand(procName, conn);
                cmd.CommandType = CommandType.StoredProcedure;

                int rows_affected = cmd.ExecuteNonQuery();
                Console.WriteLine($"{rows_affected} by {procName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error Running Stored Proc: {procName}" + 
                    Environment.NewLine +
                    e.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        public override string ToString()
        {
            return this.ConnString;
        }
    }
}