using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion
{
    public class MessageContext : IDisposable
    {
        public IngestionContext IngestionContext { get; private set; }

        public JObject Message { get; private set; }

        public Lazy<Dictionary<string, JProperty>> MessageProperties => new Lazy<Dictionary<string,JProperty>>(() => Message.Properties().ToDictionary(p => p.Name, p => p));

        public void SetMessage(JObject item)
        {
            var messagePayLoad = item.SelectToken("message", false);
            if (messagePayLoad != null) Message = messagePayLoad as JObject;
            else Message = item;
        }

        private MessageContext(){}

        public static MessageContext FromIngestionContext(IngestionContext context, JObject item)
        {
            var mContext = new MessageContext() { IngestionContext = context };
            mContext.SetMessage(item);
            return mContext;
        }

        public void Dispose()
        {
            Message = null;
        }
    }
}