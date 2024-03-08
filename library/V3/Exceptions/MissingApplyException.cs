namespace ServcoX.EventSauce.V3.Exceptions;

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