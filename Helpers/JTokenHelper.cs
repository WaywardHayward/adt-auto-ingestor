using System;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Helpers
{
    public static class JTokenHelper
    {
        /// <summary>
        /// Gets the value of the specified token as ISO 8601 date and time 
        /// </summary>
        public static string SelectDateTimeTokenString(this JToken token, string path )
        {
            var value = token.SelectToken(path, false)?.Value<DateTime>();
            if(value == null) return null;
            return value?.ToString("o");            
        }

    }
}