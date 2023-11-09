using System;
using System.IO;
using System.Runtime.InteropServices;

namespace CSharpFileIODemo
{
	/// <summary>
	/// Summary description for TestStruct.
	/// </summary>
	[StructLayout(LayoutKind.Sequential, Pack=1)]
	public struct x
	{
		public long longField;
		public byte byteField;
		[MarshalAs(UnmanagedType.ByValArray, SizeConst=16)]
		public byte[] byteArrayField;
		public float floatField;

		public static x FromBinaryReaderBlock(BinaryReader br)
		{
			byte[] buff = br.ReadBytes(TSSize.Size);//faster than (Marshal.SizeOf(typeof(TestStruct)));
			GCHandle handle = GCHandle.Alloc(buff, GCHandleType.Pinned);
			x s = (x)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(x));
			handle.Free();
			return s;
		}

		public static x FromBinaryReaderField(BinaryReader br)
		{
			x s = new x();
			s.longField = br.ReadInt64();
			s.byteField = br.ReadByte();
			s.byteArrayField = br.ReadBytes(16);
			s.floatField = br.ReadSingle();
			return s;
		}

		public static x FromFileStream(FileStream fs)
		{
			byte[] buff = new byte[TSSize.Size];//faster than [Marshal.SizeOf(typeof(TestStruct))];
			int amt = 0;
			while(amt < buff.Length)
				amt += fs.Read(buff, amt, buff.Length-amt);
			GCHandle handle = GCHandle.Alloc(buff, GCHandleType.Pinned);
			Action s = (x)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(x));
			handle.Free();
			return s;
		}

		public static x FromRandom(Random rnd)
		{
			byte[] buff = new byte[TSSize.Size];//faster than [Marshal.SizeOf(typeof(TestStruct))];
			rnd.NextBytes(buff);
			GCHandle handle = GCHandle.Alloc(buff, GCHandleType.Pinned);
			x s = (x)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(x));
			handle.Free();
			return s;
		}

		public byte[] ToByteArray()
		{
			byte[] buff = new byte[TSSize.Size];//faster than [Marshal.SizeOf(typeof(TestStruct))];
			GCHandle handle = GCHandle.Alloc(buff, GCHandleType.Pinned);
			Marshal.StructureToPtr(this, handle.AddrOfPinnedObject(), false);
			handle.Free();
			return buff;
		}
	}

	internal sealed class TSSize
	{
		public static int _size;

		static TSSize()
		{
			_size = Marshal.SizeOf(typeof(x));
		}

		public static int Size
		{
			get
			{
				return _size;
			}
		}
	}
}
