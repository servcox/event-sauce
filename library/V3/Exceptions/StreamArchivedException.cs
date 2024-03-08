namespace ServcoX.EventSauce.V3.Exceptions;

public class StreamArchivedException : Exception
{
    public StreamArchivedException()
    {
    }

    public StreamArchivedException(String message) : base(message)
    {
    }

    public StreamArchivedException(String message, Exception innerException) : base(message, innerException)
    {
    }
}