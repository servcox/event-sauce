namespace ServcoX.EventSauce.V3.Exceptions;

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