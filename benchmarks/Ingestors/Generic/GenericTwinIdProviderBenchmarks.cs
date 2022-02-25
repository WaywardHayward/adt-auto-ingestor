using System.Text;
using adt_auto_ingester.Ingestion;
using adt_auto_ingester.Ingestion.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Xunit;

namespace benchmarks.Ingestors.Generic
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn()]
    public class GenericTwinIdProviderBenchmarks : BaseBenchmarks
    {

        private string _identifierPaths = "deviceId;DeviceId";
        private string _pathWithId = "DeviceId";
        private string _deviceId = "MyDeviceId";
        private bool _expectMatch = true;

        private MessageContext _messageContext = null;
        private IConfiguration _configuration = null;
        private GenericMessageTwinIdProvider _twinIdProvider;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _configuration = GetConfigurationWith(_identifierPaths);
            var loggerFactory = new LoggerFactory();
            var ingestionContext = GetIngestionContext(_configuration, loggerFactory);
           
            var message = new JObject()
            {
                [_pathWithId ?? "notSpecified"] = _deviceId
            };

            ingestionContext.SetIngestionMessage(new EventData(Encoding.UTF8.GetBytes(message.ToString())));
            _messageContext = MessageContext.FromIngestionContext(ingestionContext, message);
            _twinIdProvider =  new GenericMessageTwinIdProvider(_configuration, new Logger<GenericMessageTwinIdProvider>(loggerFactory));
        }

        [Benchmark]
        
        public void BenchmarkIdProvider(){

            var result = _twinIdProvider.PopulateTwinId(_messageContext);

            if (_expectMatch)
                Assert.Equal(_deviceId, result);
            else
                Assert.NotEqual(_deviceId, result);
        }
    }
}