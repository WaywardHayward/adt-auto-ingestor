using System;
using adt_auto_ingester.AzureDigitalTwins.Face;
using Azure.Core;
using Azure.DigitalTwins.Core;

namespace adt_auto_ingester.AzureDigitalTwins
{

    public class DigitalTwinsClientProvider : IDigitalTwinsClientProvider
    {
        private readonly Uri _url;
        private readonly TokenCredential _credential;

        public DigitalTwinsClientProvider(Uri url, TokenCredential credential)
        {
            _url = url;
            _credential = credential;
        }

        public DigitalTwinsClient GetClient()
        {
            return new DigitalTwinsClient(_url,_credential);
        }
    }
}