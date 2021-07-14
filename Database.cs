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
        public MySqlConnection Conn { get; set; }

        public Database()
        {
            string server = Environment.GetEnvironmentVariable("alison_database_server");
            string name = Environment.GetEnvironmentVariable("alison_database_name");
            string port = Environment.GetEnvironmentVariable("alison_database_port");
            string username = Environment.GetEnvironmentVariable("alison_database_username");
            string password = Environment.GetEnvironmentVariable("alison_database_password");
            Console.WriteLine("got the datbase " + server);

            this.ConnString = $@"server = {server};user={username};database={name};port={port};password={password};";
            this.Conn = new MySqlConnection(this.ConnString);
        }

        public void Open()
        {
            this.Conn.Open();
        }

        public void Close()
        {
            this.Conn.Close();
        }

        public List<ExpandoObject> Select(string query)
        {
            List<ExpandoObject> results = new();
            try
            {
                using var cmd = new MySqlCommand(query, this.Conn);
                using var rdr = cmd.ExecuteReader();
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

            return results;
        }

        public void Insert(string query, Dictionary<string, object> values)
        {
            try
            {
                using var cmd = new MySqlCommand(query, this.Conn);
                foreach (var p in values)
                {
                    cmd.Parameters.AddWithValue(p.Key, p.Value);
                }

                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error Inserting Data");
                Console.WriteLine(e.Message);
            }
        }

        public void StoredProc(string procName)
        {
            try
            {
                using var cmd = new MySqlCommand(procName, this.Conn);
                cmd.CommandType = CommandType.StoredProcedure;
                int rows_affected = cmd.ExecuteNonQuery();
                Console.WriteLine($"{rows_affected} Rows Effected By {procName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error Running Stored Proc: {procName}" +
                    Environment.NewLine + e.Message);
            }
        }
    }
}