using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace etl
{
    public class Database
    {
        public string ConnString { get; set; }

        public Database(Config config)
        {
            string server = config.database_server;
            string database = config.database_name;
            string port = config.database_port;
            string username = config.database_username;
            string password = config.database_password;

            this.ConnString = $@"server = {server};user={username};database={database};port={port};password={password};";
        }

        public List<ExpandoObject> Select(string fields, string table, string where=null)
        {

            using var conn = new MySqlConnection(this.ConnString);
            string stm = $"select {fields} from {table}";
            if (where != null) { stm += $" where {where}"; }

            List<ExpandoObject> data = new List<ExpandoObject>();

            try{
                conn.Open();
                using var cmd2 = new MySqlCommand(stm, conn);

                using (var rdr = cmd2.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        dynamic temp = new ExpandoObject();
                        temp = rdr;
                        data.Add(temp);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return data;
        }

        public override string ToString()
        {
            return this.ConnString;
        }
    }
}