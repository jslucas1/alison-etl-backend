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

        public override string ToString()
        {
            return this.ConnString;
        }
    }
}