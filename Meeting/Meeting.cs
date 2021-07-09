using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace etl.Meeting
{
    public class Meeting
    {
        public int Id { get; set; }
        public string Body { get; set; }
        public string EventType { get; set; }
        public string EventDate { get; set; }
        public string EventTime { get; set; }
        public string DeadlineDate { get; set; }
        public string DeadlineTime { get; set; }
        public string Location { get; set; }
        public string EventTitle { get; set; }
        public string EventDescription { get; set; }

        public override string ToString()
        {
            return $"ID: {this.Id}" + Environment.NewLine +
                $"Body: {this.Body}" + Environment.NewLine +
                $"Type: {this.EventType}" + Environment.NewLine +
                $"DateTime: {this.EventDate} {this.EventTime}" + Environment.NewLine +
                $"Deadline: {this.DeadlineDate} {this.DeadlineTime}" + Environment.NewLine +
                $"Location: {this.Location}" + Environment.NewLine +
                $"Title: {this.EventTitle}" + Environment.NewLine +
                $"Description: {this.EventDescription}";
        }
    }
}
