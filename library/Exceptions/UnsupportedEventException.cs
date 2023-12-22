namespace ServcoX.EventSauce.Exceptions;

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