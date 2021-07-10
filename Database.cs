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

        public Database(string server, string name, string port, string username, string password)
        {
            this.ConnString = $@"server = {server};user={username};database={name};port={port};password={password};";
        }

        public Database()
        {
            string server = Environment.GetEnvironmentVariable("alison_database_server");
            string name = Environment.GetEnvironmentVariable("alison_database_name");
            string port = Environment.GetEnvironmentVariable("alison_database_port");
            string username = Environment.GetEnvironmentVariable("alison_database_username");
            string password = Environment.GetEnvironmentVariable("alison_database_password");

            this.ConnString = $@"server = {server};user={username};database={name};port={port};password={password};";
        }

        public override string ToString()
        {
            return this.ConnString;
        }
    }
}