namespace ServcoX.EventSauce.Exceptions;

public class MissingIndexException : Exception
{
    public MissingIndexException()
    {
    }

    public MissingIndexException(String message) : base(message)
    {
    }

    public MissingIndexException(String message, Exception innerException) : base(message, innerException)
    {
    }
}