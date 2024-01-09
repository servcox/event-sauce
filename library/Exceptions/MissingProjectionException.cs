namespace ServcoX.EventSauce.Exceptions;

public class MissingProjectionException : Exception
{
    public MissingProjectionException()
    {
    }

    public MissingProjectionException(String message) : base(message)
    {
    }

    public MissingProjectionException(String message, Exception innerException) : base(message, innerException)
    {
    }
}