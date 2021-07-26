using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Face
{
    public interface IMessageIngestor
    {
        Task Ingest(EventData eventData, JObject message);
    }
}