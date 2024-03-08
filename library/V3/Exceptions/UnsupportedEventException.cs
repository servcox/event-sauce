namespace ServcoX.EventSauce.V3.Exceptions;

public class UnsupportedEventException : Exception
{
    public UnsupportedEventException()
    {
    }

    public UnsupportedEventException(String message) : base(message)
    {
    }

    public UnsupportedEventException(String message, Exception innerException) : base(message, innerException)
    {
    }
}