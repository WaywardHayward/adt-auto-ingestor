using Microsoft.Azure.EventHubs;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Face
{
    public interface ITwinIdProvider
    {
         string PopulateTwinId(MessageContext context);
    }
}