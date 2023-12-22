namespace ServcoX.EventSauce;

public class OptimisticWriteInterruptedException :Exception
{
    public OptimisticWriteInterruptedException()
    {
    }
    
    public OptimisticWriteInterruptedException(string message) : base(message)
    {
    }

    public OptimisticWriteInterruptedException(string message, Exception innerException) : base(message, innerException)
    {
    }
}