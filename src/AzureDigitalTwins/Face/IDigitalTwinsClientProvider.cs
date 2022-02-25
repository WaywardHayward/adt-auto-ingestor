using Azure.DigitalTwins.Core;

namespace adt_auto_ingester.AzureDigitalTwins.Face
{
    public interface IDigitalTwinsClientProvider
    {
        DigitalTwinsClient GetClient();
    }
}