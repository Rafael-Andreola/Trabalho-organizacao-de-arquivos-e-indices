using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Trabalho1_OrganizaçõesDeArquivosE_Indices.Class
{
    public class Row
    {
        public long Id {  get; set; }
        public string productId { get; set; }
        public string categoryId { get; set; }
        public string brand { get; set; }
        public string userId { get; set; }
        public string userSession { get; set; }
        public string eventType { get; set; }
        public string deleteField { get; set; }

    }
}
