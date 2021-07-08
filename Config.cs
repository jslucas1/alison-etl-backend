using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace etl
{
    public class Config
    {

        public string database_server { get; set; }
        public string database_name { get; set; }
        public string database_port { get; set; }
        public string database_username { get; set; }
        public string database_password { get; set; }
        public string api_url { get; set; }
        public string api_username { get; set; }
        public string api_password { get; set; }

        public Config()
        {
            string os = Environment.OSVersion.ToString();

            LoadDBConfig(os);
            // LoadAPIConfig(os);
        }

        // TODO: read from env
        private void LoadDBConfig(string os)
        {
            this.database_server = Environment.GetEnvironmentVariable("alison_database_server");
            this.database_name = Environment.GetEnvironmentVariable("alison_database_name");
            this.database_port = Environment.GetEnvironmentVariable("alison_database_port");
            this.database_username = Environment.GetEnvironmentVariable("alison_database_username");
            this.database_password = Environment.GetEnvironmentVariable("alison_database_password");
        }

        // TODO: read from env
        private void LoadAPIConfig(string os)
        {
            this.api_url = Environment.GetEnvironmentVariable("alison_api_url");
            this.api_username = Environment.GetEnvironmentVariable("alison_api_username");
            this.api_password = Environment.GetEnvironmentVariable("alison_api_password");
        }

    }
}
