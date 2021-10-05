using System;
using System.IO;
using System.Runtime.InteropServices;

namespace StorageConsole
{
	/// <summary>
	/// Summary description for TestStruct.
	/// </summary>
	[StructLayout(LayoutKind.Sequential, Pack=1)]
	public struct TestStruct
	{
		int _intData;
		//[MarshalAs(UnmanagedType.LPStr,SizeConst = 16)]
		//string _stringData;
		[MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
		char[] _chardata;

		public int Int
		{
			set
			{
				_intData = value;
			}
			get
			{
				return (_intData);
			}
		}

        //public string String
        //{
        //    set
        //    {
        //        _stringData = value;
        //    }
        //    get
        //    {
        //        return (_stringData);
        //    }
        //}

        public char[] Chars
        {
            set
            {
                _chardata = value;
            }
            get
            {
                return (_chardata);
            }
        }

        public byte[] ToByteArray()
		{
			byte[] buff = new byte[TSSize.Size];//faster than [Marshal.SizeOf(typeof(TestStruct))];
			//byte[] buff = new byte[Marshal.SizeOf(typeof(TestStruct))];
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
			_size = Marshal.SizeOf(typeof(TestStruct));
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
