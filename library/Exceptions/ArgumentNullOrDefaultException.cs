namespace ServcoX.EventSauce.Exceptions;

public class ArgumentNullOrDefaultException : Exception
{
    public ArgumentNullOrDefaultException()
    {
    }

    public ArgumentNullOrDefaultException(String message) : base(message)
    {
    }

    public ArgumentNullOrDefaultException(String message, Exception innerException) : base(message, innerException)
    {
    }
}