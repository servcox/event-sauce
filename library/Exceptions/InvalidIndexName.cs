namespace ServcoX.EventSauce.Exceptions;

public class InvalidIndexName : Exception
{
    public InvalidIndexName()
    {
    }

    public InvalidIndexName(String message) : base(message)
    {
    }

    public InvalidIndexName(String message, Exception innerException) : base(message, innerException)
    {
    }
}