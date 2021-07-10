using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace etl
{
    public class ApiManager
    {
        public string ApiUrl { get; set; }
        public string ApiUsername { get; set; }
        public string ApiPassword { get; set; }

        public ApiManager()
        {
            ApiUrl = Environment.GetEnvironmentVariable("alison_api_url");
            ApiUsername = Environment.GetEnvironmentVariable("alison_api_username");
            ApiPassword = Environment.GetEnvironmentVariable("alison_api_password");
        }

        public ApiManager(string url, string username, string password)
        {
            ApiUrl = url;
            ApiUsername = username;
            ApiPassword = password;
            
        }
    }
}
