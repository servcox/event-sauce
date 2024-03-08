namespace ServcoX.EventSauce.V3.Exceptions;

public class NotFoundException : Exception
{
    public NotFoundException()
    {
    }

    public NotFoundException(String message) : base(message)
    {
    }

    public NotFoundException(String message, Exception innerException) : base(message, innerException)
    {
    }
}