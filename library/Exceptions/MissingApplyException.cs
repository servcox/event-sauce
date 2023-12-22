namespace ServcoX.EventSauce.Exceptions;

public class MissingApplyException : Exception
{
    public MissingApplyException()
    {
    }

    public MissingApplyException(String message) : base(message)
    {
    }

    public MissingApplyException(String message, Exception innerException) : base(message, innerException)
    {
    }
}