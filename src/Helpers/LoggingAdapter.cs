using System;
using Microsoft.Extensions.Logging;
using src.Helpers.Face;

namespace adt_auto_ingester.Helpers
{
    public class LoggingAdapter : ILoggerAdapter<ILogger>
    {
        private static LoggingAdapter _adapter;
        private ILogger _logger;
        public LoggingAdapter()
        {
            _logger = new LoggerFactory().CreateLogger<ILogger>();
            _adapter = this;
        }

        public void LogInformation(string message)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(message);
            }
        }

        public void LogInformation<T0>(string message, T0 arge)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(message, 0);
            }
        }

        public void LogInformation<T0, T1>(string message, T0 argo, T1 arg1)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(message, argo, arg1);
            }
        }

        public void LogInformation<T0, T1, T2>(string message, T0 argo, T1 arg1, T2 arg2)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(message, argo, arg1);
            }
        }

        public void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        public void LogError(string message)
        {
            _logger.LogError(message);
        }

        public void LogError(Exception ex, string message)
        {
            _logger.LogError(ex, message);
        }

        public void LogDebug(string message)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(message);
            }
        }

        public void LogWarning(string message)
        {
            if (_logger.IsEnabled(LogLevel.Warning))
            {
                _logger.LogWarning(message);
            }
        }
    }
}