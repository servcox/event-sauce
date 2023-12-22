namespace ServcoX.EventSauce.Exceptions;

public class NeverNullException : Exception
{
    public NeverNullException()
    {
    }
    
    public NeverNullException(string message) : base(message)
    {
    }

    public NeverNullException(string message, Exception innerException) : base(message, innerException)
    {
    }
}