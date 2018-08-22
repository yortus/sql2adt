/* Derived from code from https://github.com/albertzak/node_adt, with the following license:
 *     Copyright (c) 2010-2010 Chase Gray mailto:chase@ratchetsoftware.com
 *     Copyright (c) 2015 Albert Zak mailto:me@albertzak.com
 *     Copyright (c) 2018 Troy Gerwien mailto:yortus@gmail.com
 *
 *     Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 *     documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 *     the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 *     to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 *     The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 *     the Software.
 *
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 *     THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 *     CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *     IN THE SOFTWARE.
 */




import * as fs from 'fs';
import * as iconv from 'iconv-lite';
import {promisify} from 'util';





// TODO: added...
const fsOpen = promisify(fs.open);
const fsRead = promisify(fs.read);
const fsClose = promisify(fs.close);




const MS_PER_DAY = 1000 * 60 * 60 * 24;
const JULIAN_1970 = 2440588;




// TODO: added...
const HEADER_LENGTH = 400;
const COLUMN_LENGTH = 200;
const enum ColumnType {
    LOGICAL       = 1,
    CHARACTER     = 4,
    CICHARACTER   = 20,
    NCHAR         = 26,
    DOUBLE        = 10,
    INTEGER       = 11,
    AUTOINCREMENT = 15,
    SHORT         = 12,
    DATE          = 3,
    TIME          = 13,
    TIMESTAMP     = 14,
}




// TODO: added...
interface Header {
    recordCount: number;
    dataOffset: number;
    recordLength: number;
    columnCount: number;
}
interface Column {
    name: string;
    type: ColumnType;
    length: number;
}
export interface Record {
    [columnName: string]: unknown;
}




export class AdtFile {

    // TODO: jsdoc...
    static async open(path: string, encoding: string) {
        encoding = encoding || 'ISO-8859-1';
        let fd = await fsOpen(path, 'r');
        let header = await this.parseHeader(fd);
        let columns = await this.parseColumns(fd, encoding, header);
        return new AdtFile(encoding, fd, header, columns);
    }

    // TODO: jsdoc...
    async fetchRecords(options?: {offset?: number, limit?: number}) {
        options = options || {};

        // Calculate iteration limits
        let startingIndex = typeof options.offset === 'number' ? options.offset : 0;
        if (startingIndex < 0) startingIndex = 0;
        if (startingIndex > this.header.recordCount) startingIndex = this.header.recordCount;
        let finishedIndex =  typeof options.limit === 'number' ? startingIndex + options.limit : this.header.recordCount;
        if (finishedIndex < startingIndex) finishedIndex = startingIndex;
        if (finishedIndex > this.header.recordCount) finishedIndex = this.header.recordCount;

        let records = [] as Record[];
        let iteratedCount = 0;
        while (startingIndex + iteratedCount < finishedIndex) {
            let start  = this.header.dataOffset + this.header.recordLength * (startingIndex + iteratedCount);
            let end    = start + this.header.recordLength;
            let length = end - start;
            let tempBuffer = new Buffer(length);

            let {buffer} = await fsRead(this.fd, tempBuffer, 0, length, start);

            // TODO: desired semantics for deleted records????
            // read next record, unless it is marked as deleted (first byte = 0x05)
            if (buffer.readInt8(0) !== 5) {
                records.push(this.parseRecord(buffer));
            }

            ++iteratedCount;
        }
        return records;
    }

    async fetchRecord(recordNumber: number) {
        if (recordNumber > this.header.recordCount) {
            throw new Error(`record number ${recordNumber} is greater than the record count (${this.header.recordCount})`);
        }

        let start  = this.header.dataOffset + this.header.recordLength * recordNumber;
        let end    = start + this.header.recordLength;
        let length = end - start;
        let tempBuffer = new Buffer(length);

        let {buffer} = await fsRead(this.fd, tempBuffer, 0, length, start);
        let record = this.parseRecord(buffer);
        return record;
    }

    async close() {
        if (this.fd !== -1) {
            let closed = fsClose(this.fd);
            this.fd = -1;
            await closed;
        }
    };

    private constructor(
        private encoding: string,
        private fd: number,
        private header: Header,
        private columns: Column[]
    ) {}

    // Determine record count, column count, record length, and data offset
    private static async parseHeader(fd: number) {
        let {buffer} = await fsRead(fd, new Buffer(HEADER_LENGTH), 0, HEADER_LENGTH, 0);
        let header = {} as Header;

        header.recordCount  = buffer.readUInt32LE(24);
        header.dataOffset   = buffer.readUInt32LE(32);
        header.recordLength = buffer.readUInt32LE(36);
        header.columnCount  = (header.dataOffset - 400) / 200;
        return header;
    }

    // Retrieves column information from the database
    private static async parseColumns(fd: number, encoding: string, header: Header) {
        // column information is located after the header
        // 200 bytes of information for each column
        let columnsLength = COLUMN_LENGTH * header.columnCount;
        let tempBuffer = new Buffer(HEADER_LENGTH + columnsLength);
        let {buffer} = await fsRead(fd, tempBuffer, 0, columnsLength, HEADER_LENGTH);

        let columns = [] as Column[];

        for (var i = 0; i < header.columnCount; ++i) {
            var column = buffer.slice(COLUMN_LENGTH * i);

            // column names are the first 128 bytes and column info takes up the last 72 bytes.
            // byte 130 contains a 16-bit column type
            // byte 136 contains a 16-bit length field
            var name = iconv.decode(column.slice(0, 128), encoding).replace(/\0/g, '').trim();
            var type = column.readUInt16LE(129);
            var length = column.readUInt16LE(135);
            columns.push({name: name, type: type, length: length});
        }
        return columns;
    }

    private parseRecord(buffer: Buffer) {

        // skip the first 5 bytes, don't know what they are for and they don't contain the data.
        buffer = buffer.slice(5);

        var record = {} as Record;
        var offset = 0;

        for (var i = 0; i < this.header.columnCount; ++i) {
            var start = offset;
            var end = offset + this.columns[i].length;
            var field = buffer.slice(start, end);
            record[this.columns[i].name] = this.parseField(field, this.columns[i].type, this.columns[i].length);
            offset += this.columns[i].length;
        }

        return record;
    }

    // Reference:
    // http://devzone.advantagedatabase.com/dz/webhelp/advantage8.1/server1/adt_field_types_and_specifications.htm
    private parseField(buffer: Buffer, type: number, length: number) {
        var value;

        switch(type) {
            case ColumnType.CHARACTER:
            case ColumnType.CICHARACTER:
                value = iconv.decode(buffer, this.encoding).replace(/\0/g, '').trim();
                break;

            case ColumnType.NCHAR:
                value = buffer.toString('ucs2', 0, length).replace(/\0/g, '').trim();
                break;

            case ColumnType.DOUBLE:
                value = buffer.readDoubleLE(0);
                break;

            case ColumnType.AUTOINCREMENT:
                value = buffer.readUInt32LE(0);
                break;

            case ColumnType.INTEGER:
                value = buffer.readInt32LE(0);
                if (value === -2147483648) value = null;
                break;

            case ColumnType.SHORT:
                value = buffer.readInt16LE(0);
                break;

            case ColumnType.LOGICAL:
                var b = iconv.decode(buffer, this.encoding);
                value = (b === 'T' || b === 't' || b === '1' || b === 'Y' || b === 'y' || b === ' ')
                break;

            case ColumnType.DATE:
                var julian = buffer.readInt32LE(0);
                value = julian === 0 ? null : new Date((julian - JULIAN_1970) * MS_PER_DAY);
                break;

            case ColumnType.TIMESTAMP:
                var julian = buffer.readInt32LE(0);
                var ms = buffer.readInt32LE(4);
                value = julian === 0 && ms === -1 ? null : new Date((julian - JULIAN_1970) * MS_PER_DAY + ms);
                break;

            // not implemented
            case ColumnType.TIME:
                value = buffer;
                break;

            default:
                value = null;
        }

        return value;
    }
}