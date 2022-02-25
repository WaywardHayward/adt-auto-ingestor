using adt_auto_ingester.Ingestion.Face;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.TwinIQ
{
    public class TiqTwinIdProvider : ITwinIdProvider
    {
        public string PopulateTwinId(MessageContext context)
        {
            var nodeId = context.Message.SelectToken("Routing.TiqTwin.NodeId", true);
            return nodeId != null ? nodeId.Value<string>() : string.Empty;
        }

    }
}