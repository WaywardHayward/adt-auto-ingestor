using System;
using adt_auto_ingester.Helpers;
using Newtonsoft.Json.Linq;
using Xunit;

namespace adt_auto_ingester_tests
{

    public class JTokenHelperTests
    {
        [Fact]
        public void GivenAJTokenWithADateTime_SelectDateTimeTokenString_ReturnsTheCorrectValue()
        {
            var timeValue = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            var testToken = JToken.Parse($"{{\"time\": \"{timeValue}\"}}");
            var result = testToken.SelectDateTimeTokenString("time");
            Assert.Equal(timeValue, result);
        }

        [Fact]
        public void GivenAJTokenWithAMissingKey_SelectDateTimeTokenString_ReturnsNull()
        {
            var timeValue = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            var testToken = JToken.Parse($"{{\"Fred\": \"{timeValue}\"}}");
            var result = testToken.SelectDateTimeTokenString("time");
            Assert.Null(result);
        }
    }

}