using System.Text;

namespace ServcoX.EventSauce.V3;

public class StreamLineReader(Stream stream)
{
    private const Int32 BufferLength = 1024;

    private readonly Byte[] _buffer = new Byte[BufferLength];
    private Int32 _bufferCount;
    private Int32 _bufferOffset;

    public Int64 Position { get; private set; }
    public Int32 Line { get; private set; }

    public String? TryReadLine()
    {
        var found = false;

        var sb = new StringBuilder();
        while (!found)
        {
            if (_bufferCount <= 0)
            {
                _bufferOffset = 0;
                _bufferCount = stream.Read(_buffer, 0, BufferLength);
                if (_bufferCount == 0)
                {
                    if (sb.Length > 0) break;
                    return null;
                }
            }

            for (var max = _bufferOffset + _bufferCount; _bufferOffset < max;)
            {
                var ch = (Char)_buffer[_bufferOffset]; // TODO: Problem with non-ASCII characters?
                _bufferCount--;
                _bufferOffset++;
                Position++;

                if (ch == '\n')
                {
                    found = true;
                    break;
                }

                sb.Append(ch);
            }
        }

        Line++;
        return sb.ToString();
    }
}