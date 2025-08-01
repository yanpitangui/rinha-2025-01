using Microsoft.IO;

namespace Rinha.Api;

public static class MemoryStreamManager
{
    public static readonly RecyclableMemoryStreamManager Manager = new RecyclableMemoryStreamManager();
    public record PooledBuffer(byte[] Buffer, int Length);
}