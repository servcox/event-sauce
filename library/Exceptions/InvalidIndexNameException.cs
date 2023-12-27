namespace ServcoX.EventSauce.Exceptions;

public class InvalidIndexNameException : Exception
{
    public InvalidIndexNameException()
    {
    }

    public InvalidIndexNameException(String message) : base(message)
    {
    }

    public InvalidIndexNameException(String message, Exception innerException) : base(message, innerException)
    {
    }
}