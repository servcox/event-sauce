namespace ServcoX.EventSauce.V3.Exceptions;

public class AlreadyExistsException : Exception
{
    public AlreadyExistsException()
    {
    }

    public AlreadyExistsException(String message) : base(message)
    {
    }

    public AlreadyExistsException(String message, Exception innerException) : base(message, innerException)
    {
    }
}