// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using adt_auto_ingester.Helpers;
using adt_auto_ingester.Ingestion;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Microsoft.Adt.AutoIngestor
{
    public class EventHubIngestor
    {

        private readonly IConfiguration _configuration;
        private readonly MessageIngestorFactory _ingestorFactory;
        private readonly IngestionContext _context;
        private readonly LoggingAdapter _loggingAdapter;

        public EventHubIngestor(IConfiguration configuration, MessageIngestorFactory ingestorFactory, IngestionContext context, LoggingAdapter adapter)
        {
            _configuration = configuration;
            _ingestorFactory = ingestorFactory;
            _context = context;
            _loggingAdapter = adapter;
            
        }

        [FunctionName("EventHubIngestor")]
        public async Task Run([EventHubTrigger("%INGESTION_EVENTHUB_NAME%", Connection = "INGESTION_EVENTHUB_CONNECTION_STRING", ConsumerGroup = "%INGESTION_EVENTHUB_CONSUMERGROUP%")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            _loggingAdapter.SetLogger(log);
            
            for (int i = 0; i < events.Length; i++)
            {
                using (var eventData = events[i])
                {
                    try
                    {
                        _context.SetIngestionMessage(eventData);
                        var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                        var messages = GetMessageArray(messageBody);

                        for (int messageIndex = 0; messageIndex < messages.Count; messageIndex++)
                        {
                            await ProcessMessage(log, messages, messageIndex);
                        }


                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();

        }

        private static JArray GetMessageArray(string messageBody)
        {
            JArray messages = new JArray();

            if (messageBody.StartsWith("["))
                messages = JArray.Parse(messageBody);
            else
                messages.Add(JObject.Parse(messageBody));
            return messages;
        }

        private async Task ProcessMessage(ILogger log, JArray messages, int i1)
        {
            JToken message = messages[i1];
            var item = message as JObject;
            using (var messageContext = MessageContext.FromIngestionContext(_context, item))
            {
                var ingestor = _ingestorFactory.Build(messageContext);

                if (ingestor != null)
                {
                    await ingestor?.Ingest(messageContext);
                }
                else
                {
                    log.LogWarning("No ingestor for message - ignoring...");
                }
            }
        }
    }
}
