using System;

namespace src.Helpers.Face
{
    public interface ILoggerAdapter<T>
    {
        void LogInformation(string message);
        void LogInformation<T0>(string message, T0 arge);
        void LogInformation<T0, T1>(string message, T0 argo, T1 arg1);
        void LogInformation<T0, T1, T2>(string message, T0 arge, T1 arg1, T2 arg2);

        void LogError(string message);

        void LogError(Exception ex, string message);

        void LogDebug(string message);

        void LogWarning(string message);

    }
}