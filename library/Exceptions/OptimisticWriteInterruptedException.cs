namespace ServcoX.EventSauce.Exceptions;

public class OptimisticWriteInterruptedException : Exception
{
    public OptimisticWriteInterruptedException()
    {
    }

    public OptimisticWriteInterruptedException(String message) : base(message)
    {
    }

    public OptimisticWriteInterruptedException(String message, Exception innerException) : base(message, innerException)
    {
    }
}