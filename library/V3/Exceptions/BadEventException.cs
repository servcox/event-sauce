namespace ServcoX.EventSauce.V3.Exceptions;

public class BadEventException : Exception
{
    public BadEventException()
    {
    }

    public BadEventException(String message) : base(message)
    {
    }

    public BadEventException(String message, Exception innerException) : base(message, innerException)
    {
    }
}