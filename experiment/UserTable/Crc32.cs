// Copyright (c) Damien Guard  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net
// Modifications (c) Matt Howlett 2017, https://www.matthowlett.com

using System;

namespace Howlett.Kafka.Extensions.Experiment
{

    /// <summary>
    ///     This code was originally from the copyrighted code listed above but was modified significantly
    ///     as the original code was not thread safe and did not match was was required of this driver. This
    ///     class now provides a static lib which will do the simple CRC calculation required by Kafka servers.
    /// </summary>
    internal static class Crc32Provider
    {
        public enum PolyType : uint
        {
            IEEE8023Poly = 0xEDB88320,
            CastagnoliPoly = 0x82F63B78,
            KoopmanPoly = 0xEB31D82E
        }

        public const UInt32 DefaultPolynomial = (uint)PolyType.CastagnoliPoly;
        public const UInt32 DefaultSeed = 0xffffffffu;
        private static readonly UInt32[] PolynomialTable;

        static Crc32Provider()
        {
            PolynomialTable = InitializeTable(DefaultPolynomial);
        }

        public static byte[] ComputeHash(byte[] buffer, int offset, int length)
        {
            return UInt32ToBigEndianBytes(~CalculateHash(buffer, offset, length));
        }

        private static UInt32[] InitializeTable(UInt32 polynomial)
        {
            var createTable = new UInt32[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (UInt32)i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry = entry >> 1;
                createTable[i] = entry;
            }

            return createTable;
        }

        private static UInt32 CalculateHash(byte[] buffer, int offset, int length)
        {
            var crc = DefaultSeed;
            for (var i = offset; i < length; i++)
            {
                crc = (crc >> 8) ^ PolynomialTable[buffer[i] ^ crc & 0xff];
            }
            return crc;
        }

        private static byte[] UInt32ToBigEndianBytes(UInt32 uint32)
        {
            var result = BitConverter.GetBytes(uint32);

            if (BitConverter.IsLittleEndian)
                Array.Reverse(result);

            return result;
        }
    }
}