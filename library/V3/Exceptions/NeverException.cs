namespace ServcoX.EventSauce.V3.Exceptions;

public class NeverException : Exception
{
    public NeverException()
    {
    }

    public NeverException(String message) : base(message)
    {
    }

    public NeverException(String message, Exception innerException) : base(message, innerException)
    {
    }
}