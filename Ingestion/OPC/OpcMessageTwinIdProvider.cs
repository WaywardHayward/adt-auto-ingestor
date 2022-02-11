
using adt_auto_ingester.Ingestion.Face;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.OPC
{

    public class OpcMessageTwinIdProvider : ITwinIdProvider
    {
        public string PopulateTwinId(MessageContext context)
        {
            var nodeId = context.Message.SelectToken("Routing.OpcMessage.NodeId", true);
            return nodeId != null ? nodeId.Value<string>() : string.Empty;
        }
    }

}