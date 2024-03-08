namespace ServcoX.EventSauce;

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