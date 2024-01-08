namespace ServcoX.EventSauce.Exceptions;

public class NullPayloadException : Exception
{
    public NullPayloadException()
    {
    }

    public NullPayloadException(String message) : base(message)
    {
    }

    public NullPayloadException(String message, Exception innerException) : base(message, innerException)
    {
    }
}