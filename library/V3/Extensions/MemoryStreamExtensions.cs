using System.Buffers;
using System.Text;

namespace ServcoX.EventSauce.V3.Extensions;

public static class MemoryStreamExtensions
{
    public static void WriteAsUtf8(this MemoryStream target, String value)
    {
        if (target is null) throw new ArgumentNullException(nameof(target));
        if (value is null) throw new ArgumentNullException(nameof(value));
        
        var buffer = ArrayPool<Byte>.Shared.Rent(value.Length * 4); // https://stackoverflow.com/questions/9533258/what-is-the-maximum-number-of-bytes-for-a-utf-8-encoded-character
        var actual = Encoding.UTF8.GetBytes(value, 0, value.Length, buffer, 0);
        target.Write(buffer, 0, actual);
        ArrayPool<Byte>.Shared.Return(buffer);
    }

    public static void Write(this MemoryStream target, Byte[] buffer)
    {
        if (target is null) throw new ArgumentNullException(nameof(target));
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        
        target.Write(buffer, 0, buffer.Length);
    }

    public static void Rewind(this MemoryStream target)
    {
        if (target is null) throw new ArgumentNullException(nameof(target));
        
        target.Position = 0;
    }
}